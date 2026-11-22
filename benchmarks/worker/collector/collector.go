// Package collector connects the load-generator result with the existing
// pgbench parser and the results repository.
package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/yandex/odyssey/benchmarks/worker/models"
	"github.com/yandex/odyssey/benchmarks/worker/parser"
	"github.com/yandex/odyssey/benchmarks/worker/storage"
)

type Stage string

const (
	StageCollection Stage = "collection"
	StageParsing    Stage = "parsing"
	StageStorage    Stage = "storage"
)

// Error keeps failures from running pgbench, obtaining its output, parsing it,
// and uploading it distinguishable to callers and to the run error_message.
type Error struct {
	Stage Stage
	Err   error
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %v", e.Stage, e.Err) }
func (e *Error) Unwrap() error { return e.Err }

type artifact struct {
	Run        models.BenchmarkRun `json:"run"`
	RawOutput  string              `json:"raw_output"`
	FinishedAt time.Time           `json:"finished_at"`
}

type Collector struct {
	Repository storage.Repository
	SpoolDir   string
}

func (c *Collector) Collect(ctx context.Context, run models.BenchmarkRun, raw []byte, finishedAt time.Time) error {
	if c == nil || c.Repository == nil {
		return &Error{Stage: StageStorage, Err: errors.New("repository is required")}
	}
	if run.ID == uuid.Nil {
		return &Error{Stage: StageCollection, Err: errors.New("run id is required")}
	}
	if err := c.persistArtifact(run, raw, finishedAt); err != nil {
		return &Error{Stage: StageCollection, Err: err}
	}

	if err := c.Repository.SetStatus(ctx, run.ID, "collecting", nil, nil); err != nil {
		return &Error{Stage: StageStorage, Err: err}
	}
	startedAt := time.Time{}
	if run.StartedAt != nil {
		startedAt = *run.StartedAt
	}
	parsed, err := parser.ParsePgbenchOutput(strings.NewReader(string(raw)), startedAt)
	if err != nil {
		message := (&Error{Stage: StageParsing, Err: err}).Error()
		if failErr := c.Repository.FailRun(ctx, run.ID, finishedAt, message); failErr == nil {
			// The terminal failed status is durably recorded. Keeping an invalid
			// artifact would make every worker restart fail on the same parse error.
			_ = c.removeArtifact(run.ID)
		}
		return &Error{Stage: StageParsing, Err: err}
	}
	if err := c.Repository.SetStatus(ctx, run.ID, "uploading", nil, nil); err != nil {
		return &Error{Stage: StageStorage, Err: err}
	}
	if parsed.Version != "" {
		if err := c.Repository.SetPgbenchVersion(ctx, run.ID, parsed.Version); err != nil {
			return &Error{Stage: StageStorage, Err: err}
		}
	}
	if err := c.Repository.SaveSamples(ctx, run.ID, parsed.Samples); err != nil {
		return &Error{Stage: StageStorage, Err: err}
	}
	if err := c.Repository.CompleteRun(ctx, run.ID, finishedAt); err != nil {
		return &Error{Stage: StageStorage, Err: err}
	}
	if err := c.removeArtifact(run.ID); err != nil {
		return &Error{Stage: StageCollection, Err: err}
	}
	return nil
}

// Replay retries artifacts left by a previous worker process. Artifacts are
// removed only after samples and the terminal status have been committed.
func (c *Collector) Replay(ctx context.Context) error {
	if c == nil || c.SpoolDir == "" {
		return nil
	}
	entries, err := os.ReadDir(c.SpoolDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read result spool: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(c.SpoolDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read result artifact %q: %w", path, err)
		}
		var saved artifact
		if err := json.Unmarshal(data, &saved); err != nil {
			return fmt.Errorf("decode result artifact %q: %w", path, err)
		}
		if err := c.Repository.CreateRun(ctx, saved.Run); err != nil {
			return err
		}
		if err := c.Collect(ctx, saved.Run, []byte(saved.RawOutput), saved.FinishedAt); err != nil {
			return fmt.Errorf("replay %s: %w", saved.Run.ID, err)
		}
	}
	return nil
}

func (c *Collector) persistArtifact(run models.BenchmarkRun, raw []byte, finishedAt time.Time) error {
	if c.SpoolDir == "" {
		return nil
	}
	if err := os.MkdirAll(c.SpoolDir, 0o750); err != nil {
		return err
	}
	data, err := json.Marshal(artifact{Run: run, RawOutput: string(raw), FinishedAt: finishedAt})
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(c.SpoolDir, ".result-*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}()
	if err := temporary.Chmod(0o640); err != nil {
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, filepath.Join(c.SpoolDir, run.ID.String()+".json"))
}

func (c *Collector) removeArtifact(runID uuid.UUID) error {
	if c.SpoolDir == "" {
		return nil
	}
	err := os.Remove(filepath.Join(c.SpoolDir, runID.String()+".json"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
