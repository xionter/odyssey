package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yandex/odyssey/benchmarks/worker/models"
)

var ErrRunNotFound = errors.New("benchmark run not found")

type Repository interface {
	CreateRun(ctx context.Context, run models.BenchmarkRun) error
	SaveSamples(ctx context.Context, runID uuid.UUID, samples []models.BenchmarkSample) error
	SetStatus(ctx context.Context, runID uuid.UUID, status string, finishedAt *time.Time, message *string) error
	SetPgbenchVersion(ctx context.Context, runID uuid.UUID, version string) error
	CompleteRun(ctx context.Context, runID uuid.UUID, finishedAt time.Time) error
	FailRun(ctx context.Context, runID uuid.UUID, finishedAt time.Time, message string) error
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*PostgresRepository, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("database URL is empty")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse results database URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create results database pool: %w", err)
	}
	repository := &PostgresRepository{pool: pool}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping results database: %w", err)
	}
	return repository, nil
}

func NewFromEnv(ctx context.Context) (*PostgresRepository, error) {
	return New(ctx, os.Getenv("RESULTS_DATABASE_URL"))
}

func (r *PostgresRepository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

func (r *PostgresRepository) CreateRun(ctx context.Context, run models.BenchmarkRun) error {
	args := run.PgbenchArgs
	if args == nil {
		args = map[string]any{}
	}
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("marshal pgbench arguments: %w", err)
	}
	config := run.Config
	if config == nil {
		config = args
	}
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal benchmark config: %w", err)
	}
	environmentJSON, err := json.Marshal(run.Environment)
	if err != nil {
		return fmt.Errorf("marshal benchmark environment: %w", err)
	}
	var createdAt any
	if !run.CreatedAt.IsZero() {
		createdAt = run.CreatedAt
	}
	_, err = r.pool.Exec(ctx, createRunQuery,
		run.ID,
		run.CommitHash,
		run.Scenario,
		run.Status,
		run.PgbenchVersion,
		argsJSON,
		configJSON,
		environmentJSON,
		run.StartedAt,
		run.FinishedAt,
		createdAt,
		run.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("create benchmark run %s: %w", run.ID, err)
	}
	return nil
}

func (r *PostgresRepository) SaveSamples(ctx context.Context, runID uuid.UUID, samples []models.BenchmarkSample) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin save samples transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var exists bool
	if err := tx.QueryRow(ctx, benchmarkRunExistsQuery, runID).Scan(&exists); err != nil {
		return fmt.Errorf("check benchmark run %s: %w", runID, err)
	}
	if !exists {
		return fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	if _, err := tx.Exec(ctx, deleteSamplesQuery, runID); err != nil {
		return fmt.Errorf("delete existing samples for run %s: %w", runID, err)
	}

	rows := make([][]any, len(samples))
	for i, sample := range samples {
		rows[i] = []any{
			runID, sample.IntervalStart, sample.IntervalSeconds, sample.NumTransactions,
			sample.FailedTransactions,
			sample.LatencySumUS, sample.LatencySumSqUS, sample.LatencyMinUS, sample.LatencyMaxUS,
			sample.TPS, sample.LatencyAvgMS, sample.LatencyStddevMS,
		}
	}
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"benchmark_samples"},
		benchmarkSampleColumns,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("save samples for run %s: %w", runID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit samples for run %s: %w", runID, err)
	}
	return nil
}

func (r *PostgresRepository) CompleteRun(ctx context.Context, runID uuid.UUID, finishedAt time.Time) error {
	return r.SetStatus(ctx, runID, "succeeded", &finishedAt, nil)
}

func (r *PostgresRepository) FailRun(ctx context.Context, runID uuid.UUID, finishedAt time.Time, message string) error {
	return r.SetStatus(ctx, runID, "failed", &finishedAt, &message)
}

func (r *PostgresRepository) SetStatus(ctx context.Context, runID uuid.UUID, status string, finishedAt *time.Time, message *string) error {
	result, err := r.pool.Exec(ctx, updateRunStatusQuery, runID, status, finishedAt, message)
	if err != nil {
		return fmt.Errorf("update benchmark run %s status: %w", runID, err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	return nil
}

func (r *PostgresRepository) SetPgbenchVersion(ctx context.Context, runID uuid.UUID, version string) error {
	result, err := r.pool.Exec(ctx, updateRunMetadataQuery, runID, version)
	if err != nil {
		return fmt.Errorf("update benchmark run %s pgbench version: %w", runID, err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	return nil
}
