CREATE TABLE IF NOT EXISTS benchmark_runs (
    id UUID PRIMARY KEY,
    commit_hash TEXT,
    scenario TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'collecting', 'uploading', 'succeeded', 'failed', 'cancelled')),
    pgbench_version TEXT,
    pgbench_args JSONB NOT NULL DEFAULT '{}'::jsonb,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    environment JSONB,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS benchmark_samples (
    run_id UUID NOT NULL REFERENCES benchmark_runs(id) ON DELETE CASCADE,
    interval_start TIMESTAMPTZ NOT NULL,
    interval_seconds DOUBLE PRECISION NOT NULL CHECK (interval_seconds > 0),
    num_transactions BIGINT NOT NULL CHECK (num_transactions >= 0),
    failed_transactions BIGINT NOT NULL DEFAULT 0 CHECK (failed_transactions >= 0),
    latency_sum_us BIGINT NOT NULL CHECK (latency_sum_us >= 0),
    latency_sum_sq_us2 BIGINT NOT NULL CHECK (latency_sum_sq_us2 >= 0),
    latency_min_us BIGINT CHECK (latency_min_us >= 0),
    latency_max_us BIGINT CHECK (latency_max_us >= 0),
    tps DOUBLE PRECISION NOT NULL CHECK (tps >= 0),
    latency_avg_ms DOUBLE PRECISION,
    latency_stddev_ms DOUBLE PRECISION,
    PRIMARY KEY (run_id, interval_start)
);
