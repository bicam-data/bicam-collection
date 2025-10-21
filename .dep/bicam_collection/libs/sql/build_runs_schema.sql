-- SQL script to create the bicam_runs schema and its run-tracking tables
-- ---------------------------------------------------------------
-- This script is intentionally separate from `build_metadata_schema.sql`
-- so that it can be executed independently when only the run-tracking
-- schema needs to be rebuilt.  The objects defined here mirror the
-- section that previously lived inside the metadata script.

BEGIN;

-- Create bicam_runs schema
CREATE SCHEMA IF NOT EXISTS bicam_runs;

-- --------------------------------------------------------------------
-- Utility function & trigger for keeping the `updated_at` column fresh
-- --------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- --------------------------------------------------------------------
-- Primary tables
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bicam_runs.runs (
    run_id TEXT PRIMARY KEY,
    run_type TEXT NOT NULL,
    system_name TEXT NOT NULL,
    description TEXT NOT NULL,
    parameters JSONB DEFAULT '{}',
    data_types TEXT[] DEFAULT '{}',
    expected_duration_minutes INTEGER,
    priority INTEGER DEFAULT 1,
    tags TEXT[] DEFAULT '{}',
    created_by TEXT DEFAULT 'system',

    status TEXT DEFAULT 'pending',
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    progress_data JSONB DEFAULT '{}',
    output_summary JSONB DEFAULT '{}',
    resource_usage JSONB DEFAULT '{}',
    dependencies TEXT[] DEFAULT '{}',
    blocked_by TEXT[] DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bicam_runs.run_logs (
    log_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    component TEXT,
    data_type TEXT,
    extra_data JSONB DEFAULT '{}',
    FOREIGN KEY (run_id) REFERENCES bicam_runs.runs(run_id)
);

CREATE TABLE IF NOT EXISTS bicam_runs.active_locks (
    lock_name TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    locked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    lock_data JSONB DEFAULT '{}',
    FOREIGN KEY (run_id) REFERENCES bicam_runs.runs(run_id)
);

-- --------------------------------------------------------------------
-- Indexes
-- --------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_runs_status         ON bicam_runs.runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_type           ON bicam_runs.runs(run_type);
CREATE INDEX IF NOT EXISTS idx_runs_system         ON bicam_runs.runs(system_name);
CREATE INDEX IF NOT EXISTS idx_runs_created_at     ON bicam_runs.runs(created_at);
CREATE INDEX IF NOT EXISTS idx_run_logs_run_id     ON bicam_runs.run_logs(run_id);
CREATE INDEX IF NOT EXISTS idx_run_logs_timestamp  ON bicam_runs.run_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_active_locks_expires ON bicam_runs.active_locks(expires_at);

-- --------------------------------------------------------------------
-- Triggers
-- --------------------------------------------------------------------
DROP TRIGGER IF EXISTS update_runs_updated_at ON bicam_runs.runs;

CREATE TRIGGER update_runs_updated_at
    BEFORE UPDATE ON bicam_runs.runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- --------------------------------------------------------------------
-- Permissions
-- --------------------------------------------------------------------
GRANT USAGE ON SCHEMA bicam_runs TO bicam_pipeline;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA bicam_runs TO bicam_pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bicam_runs TO bicam_pipeline;

COMMIT; 