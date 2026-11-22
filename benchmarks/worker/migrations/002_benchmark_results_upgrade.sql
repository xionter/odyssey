-- Upgrade databases created with migration 001 before config/environment and
-- the collecting/uploading states were introduced.
ALTER TABLE benchmark_runs
    ADD COLUMN IF NOT EXISTS config JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS environment JSONB;

ALTER TABLE benchmark_samples
    ADD COLUMN IF NOT EXISTS failed_transactions BIGINT NOT NULL DEFAULT 0;

ALTER TABLE benchmark_runs DROP CONSTRAINT IF EXISTS benchmark_runs_status_check;
ALTER TABLE benchmark_runs
    ADD CONSTRAINT benchmark_runs_status_check
    CHECK (status IN ('queued', 'running', 'collecting', 'uploading', 'succeeded', 'failed', 'cancelled'));
