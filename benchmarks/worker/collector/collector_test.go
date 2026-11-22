package collector

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/yandex/odyssey/benchmarks/worker/models"
)

type fakeRepository struct {
	created  []models.BenchmarkRun
	statuses []string
	samples  []models.BenchmarkSample
	version  string
	complete int
}

func (r *fakeRepository) CreateRun(_ context.Context, run models.BenchmarkRun) error {
	r.created = append(r.created, run)
	return nil
}
func (r *fakeRepository) SaveSamples(_ context.Context, _ uuid.UUID, samples []models.BenchmarkSample) error {
	r.samples = append([]models.BenchmarkSample(nil), samples...)
	return nil
}
func (r *fakeRepository) SetStatus(_ context.Context, _ uuid.UUID, status string, _ *time.Time, _ *string) error {
	r.statuses = append(r.statuses, status)
	return nil
}
func (r *fakeRepository) SetPgbenchVersion(_ context.Context, _ uuid.UUID, version string) error {
	r.version = version
	return nil
}
func (r *fakeRepository) CompleteRun(_ context.Context, _ uuid.UUID, _ time.Time) error {
	r.complete++
	return nil
}
func (r *fakeRepository) FailRun(_ context.Context, _ uuid.UUID, _ time.Time, _ string) error {
	return nil
}

func TestCollectUsesParserAndRemovesArtifactAfterCommit(t *testing.T) {
	repository := &fakeRepository{}
	started := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := models.BenchmarkRun{ID: uuid.New(), Scenario: "pgbench", Status: "running", StartedAt: &started}
	collector := &Collector{Repository: repository, SpoolDir: t.TempDir()}
	raw := []byte("pgbench (16)\nprogress: 1.0 s, 10.0 tps, lat 2.0 ms stddev 0.5, 0 failed\n")

	if err := collector.Collect(context.Background(), run, raw, started.Add(time.Second)); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if repository.version != "16" || len(repository.samples) != 1 || repository.complete != 1 {
		t.Fatalf("unexpected repository calls: %+v", repository)
	}
	if len(repository.statuses) != 2 || repository.statuses[0] != "collecting" || repository.statuses[1] != "uploading" {
		t.Fatalf("statuses = %v", repository.statuses)
	}
	if _, err := os.Stat(filepath.Join(collector.SpoolDir, run.ID.String()+".json")); !os.IsNotExist(err) {
		t.Fatalf("artifact still exists, stat error: %v", err)
	}
}
