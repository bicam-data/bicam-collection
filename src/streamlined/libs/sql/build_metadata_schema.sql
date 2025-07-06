-- Create metadata schema and tables for tracking scraper progress

BEGIN;

-- Create metadata schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS bicam_metadata;

-- GovInfo last processed dates table
CREATE TABLE IF NOT EXISTS bicam_metadata.govinfo_last_processed_dates (
    data_type TEXT PRIMARY KEY,
    last_processed_date TEXT NOT NULL,
    last_total_count INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Congressional last processed dates table
CREATE TABLE IF NOT EXISTS bicam_metadata.congressional_last_processed_dates (
    data_type TEXT PRIMARY KEY,
    last_processed_date TEXT NOT NULL,
    last_total_count INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- GovInfo errors table
CREATE TABLE IF NOT EXISTS bicam_metadata.govinfo_errors (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    error TEXT NOT NULL,
    data_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Congressional errors table
CREATE TABLE IF NOT EXISTS bicam_metadata.congressional_errors (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    error TEXT NOT NULL,
    data_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_govinfo_last_processed_data_type 
    ON bicam_metadata.govinfo_last_processed_dates(data_type);

CREATE INDEX IF NOT EXISTS idx_congressional_last_processed_data_type 
    ON bicam_metadata.congressional_last_processed_dates(data_type);

CREATE INDEX IF NOT EXISTS idx_govinfo_errors_data_type 
    ON bicam_metadata.govinfo_errors(data_type);

CREATE INDEX IF NOT EXISTS idx_govinfo_errors_timestamp 
    ON bicam_metadata.govinfo_errors(timestamp);

CREATE INDEX IF NOT EXISTS idx_congressional_errors_data_type 
    ON bicam_metadata.congressional_errors(data_type);

CREATE INDEX IF NOT EXISTS idx_congressional_errors_timestamp 
    ON bicam_metadata.congressional_errors(timestamp);

-- Create triggers to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE OR REPLACE TRIGGER update_govinfo_last_processed_dates_updated_at
    BEFORE UPDATE ON bicam_metadata.govinfo_last_processed_dates
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE TRIGGER update_congressional_last_processed_dates_updated_at
    BEFORE UPDATE ON bicam_metadata.congressional_last_processed_dates
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create run tracking schema
CREATE SCHEMA IF NOT EXISTS bicam_runs;

-- Run tracking tables
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
    FOREIGN KEY(run_id) REFERENCES bicam_runs.runs(run_id)
);

CREATE TABLE IF NOT EXISTS bicam_runs.active_locks (
    lock_name TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    locked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    lock_data JSONB DEFAULT '{}',
    FOREIGN KEY(run_id) REFERENCES bicam_runs.runs(run_id)
);

-- Create indexes for run tracking
CREATE INDEX IF NOT EXISTS idx_runs_status ON bicam_runs.runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_type ON bicam_runs.runs(run_type);
CREATE INDEX IF NOT EXISTS idx_runs_system ON bicam_runs.runs(system_name);
CREATE INDEX IF NOT EXISTS idx_runs_created_at ON bicam_runs.runs(created_at);
CREATE INDEX IF NOT EXISTS idx_run_logs_run_id ON bicam_runs.run_logs(run_id);
CREATE INDEX IF NOT EXISTS idx_run_logs_timestamp ON bicam_runs.run_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_active_locks_expires ON bicam_runs.active_locks(expires_at);

-- Trigger to update updated_at on runs table
CREATE OR REPLACE TRIGGER update_runs_updated_at
    BEFORE UPDATE ON bicam_runs.runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions
GRANT USAGE ON SCHEMA bicam_metadata TO bicam_pipeline;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bicam_metadata TO bicam_pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bicam_metadata TO bicam_pipeline;

GRANT USAGE ON SCHEMA bicam_runs TO bicam_pipeline;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bicam_runs TO bicam_pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bicam_runs TO bicam_pipeline;

COMMIT;