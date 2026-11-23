package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/yandex/odyssey/benchmarks/worker/models"
)

func TestPostgresRepositoryRoundTrip(t *testing.T) {
	databaseURL := os.Getenv("RESULTS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("RESULTS_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	repository, err := New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()

	for _, name := range []string{"001_benchmark_results.sql", "002_benchmark_results_upgrade.sql"} {
		migration, err := os.ReadFile(filepath.Join("..", "migrations", name))
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := repository.pool.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}

	runID := uuid.New()
	started := time.Now().UTC().Truncate(time.Microsecond)
	run := models.BenchmarkRun{ID: runID, Scenario: "pgbench", Status: "running", StartedAt: &started, Config: map[string]any{"clients": 2}}
	if err := repository.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	samples := []models.BenchmarkSample{{
		IntervalStart: started, IntervalSeconds: 1, NumTransactions: 10, FailedTransactions: 1,
		LatencySumUS: 20, LatencySumSqUS: 40, TPS: 10,
	}}
	if err := repository.SaveSamples(ctx, runID, samples); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveSamples(ctx, runID, samples); err != nil {
		t.Fatal(err)
	}
	finished := started.Add(time.Second)
	if err := repository.CompleteRun(ctx, runID, finished); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := repository.pool.QueryRow(ctx, "SELECT count(*) FROM benchmark_samples WHERE run_id = $1", runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("sample count = %d, want 1 after retry", count)
	}
	_, _ = repository.pool.Exec(ctx, "DELETE FROM benchmark_runs WHERE id = $1", runID)
}
