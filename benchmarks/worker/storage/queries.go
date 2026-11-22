package storage

const (
	createRunQuery = `
		INSERT INTO benchmark_runs
			(id, commit_hash, scenario, status, pgbench_version, pgbench_args, config, environment, started_at, finished_at, created_at, error_message)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9, $10, COALESCE($11::timestamptz, NOW()), $12)
		ON CONFLICT (id) DO NOTHING
	`

	benchmarkRunExistsQuery = `
		SELECT EXISTS (
			SELECT 1
			FROM benchmark_runs
			WHERE id = $1
		)
	`

	deleteSamplesQuery = `
		DELETE FROM benchmark_samples
		WHERE run_id = $1
	`

	updateRunStatusQuery = `
		UPDATE benchmark_runs
		SET status = $2,
			finished_at = $3,
			error_message = $4
		WHERE id = $1
	`

	updateRunMetadataQuery = `
		UPDATE benchmark_runs
		SET pgbench_version = $2
		WHERE id = $1
	`
)

var benchmarkSampleColumns = []string{
	"run_id",
	"interval_start",
	"interval_seconds",
	"num_transactions",
	"failed_transactions",
	"latency_sum_us",
	"latency_sum_sq_us2",
	"latency_min_us",
	"latency_max_us",
	"tps",
	"latency_avg_ms",
	"latency_stddev_ms",
}
