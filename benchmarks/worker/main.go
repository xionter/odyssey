package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/yandex/odyssey/benchmarks/worker/models"
	"github.com/yandex/odyssey/benchmarks/worker/parser"
	"github.com/yandex/odyssey/benchmarks/worker/storage"
)

const defaultDatabaseURL = "postgres://benchmarks:benchmarks@127.0.0.1:5433/benchmarks?sslmode=disable"

func main() {
	inputPath := flag.String("input", "pgbench_output.txt", "pgbench output file")
	databaseURL := flag.String("database-url", os.Getenv("RESULTS_DATABASE_URL"), "results database URL")
	scenario := flag.String("scenario", "pgbench", "benchmark scenario name")
	commitHash := flag.String("commit-hash", "", "tested commit hash")
	flag.Parse()
	if *databaseURL == "" {
		*databaseURL = defaultDatabaseURL
	}

	startedAt := time.Now().UTC()
	parsed, err := parser.ParsePgbenchOutputFile(*inputPath, startedAt)
	if err != nil {
		fatal(err)
	}

	ctx := context.Background()
	repository, err := storage.New(ctx, *databaseURL)
	if err != nil {
		fatal(err)
	}
	defer repository.Close()

	runID := uuid.New()
	run := models.BenchmarkRun{
		ID:             runID,
		Scenario:       *scenario,
		Status:         "running",
		PgbenchVersion: stringPointer(parsed.Version),
		StartedAt:      &startedAt,
		CommitHash:     stringPointer(*commitHash),
	}
	if err := repository.CreateRun(ctx, run); err != nil {
		fatal(err)
	}

	if err := repository.SaveSamples(ctx, runID, parsed.Samples); err != nil {
		failRun(repository, runID, err)
		fatal(err)
	}
	finishedAt := time.Now().UTC()
	if err := repository.CompleteRun(ctx, runID, finishedAt); err != nil {
		fatal(err)
	}
	fmt.Printf("saved benchmark run %s: %d samples\n", runID, len(parsed.Samples))
}

func failRun(repository *storage.PostgresRepository, runID uuid.UUID, cause error) {
	if err := repository.FailRun(context.Background(), runID, time.Now().UTC(), cause.Error()); err != nil {
		fmt.Fprintf(os.Stderr, "also failed to mark run %s as failed: %v\n", runID, err)
	}
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
