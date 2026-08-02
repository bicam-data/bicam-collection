-- Baseline application schema. Keep schema ownership in ordered migrations;
-- runtime stores must not create or alter tables on connection.
CREATE TABLE IF NOT EXISTS crawl_runs (
    id UUID PRIMARY KEY,
    category TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    mode TEXT NOT NULL,
    high_water_mark TIMESTAMPTZ,
    items_fetched INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS raw_responses (
    id UUID PRIMARY KEY,
    endpoint_url TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    node_type TEXT NOT NULL,
    parent_id UUID REFERENCES raw_responses(id),
    response JSONB NOT NULL,
    response_hash TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    latest_crawl_run_id UUID REFERENCES crawl_runs(id)
);

CREATE INDEX IF NOT EXISTS idx_raw_responses_category ON raw_responses(source, category);
CREATE INDEX IF NOT EXISTS idx_raw_responses_node_type ON raw_responses(node_type);
CREATE INDEX IF NOT EXISTS idx_raw_responses_parent_id ON raw_responses(parent_id);
CREATE INDEX IF NOT EXISTS idx_raw_responses_latest_run ON raw_responses(latest_crawl_run_id);

CREATE TABLE IF NOT EXISTS crawl_run_fetches (
    id UUID PRIMARY KEY,
    crawl_run_id UUID NOT NULL REFERENCES crawl_runs(id),
    endpoint_url TEXT NOT NULL,
    raw_response_id UUID REFERENCES raw_responses(id),
    parent_endpoint_url TEXT,
    node_type TEXT NOT NULL,
    follow_mode TEXT NOT NULL,
    http_status INTEGER,
    outcome TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crawl_run_fetches_run_id ON crawl_run_fetches(crawl_run_id);
CREATE INDEX IF NOT EXISTS idx_crawl_run_fetches_endpoint_url ON crawl_run_fetches(endpoint_url);

CREATE TABLE IF NOT EXISTS failed_requests (
    id UUID PRIMARY KEY,
    crawl_run_id UUID NOT NULL REFERENCES crawl_runs(id),
    category TEXT NOT NULL,
    node_type TEXT NOT NULL,
    endpoint_url TEXT NOT NULL,
    parent_endpoint_url TEXT,
    http_status INTEGER,
    error TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_failed_requests_run_id ON failed_requests(crawl_run_id);
CREATE INDEX IF NOT EXISTS idx_failed_requests_endpoint_url ON failed_requests(endpoint_url);

ALTER TABLE crawl_run_fetches ADD COLUMN IF NOT EXISTS duration_ms INTEGER;
ALTER TABLE crawl_run_fetches ADD COLUMN IF NOT EXISTS attempt_count INTEGER;
ALTER TABLE crawl_run_fetches ADD COLUMN IF NOT EXISTS rate_limit_attempts INTEGER;
ALTER TABLE crawl_run_fetches ADD COLUMN IF NOT EXISTS api_key_name TEXT;
ALTER TABLE crawl_run_fetches ADD COLUMN IF NOT EXISTS pool_allocation JSONB;
ALTER TABLE failed_requests ADD COLUMN IF NOT EXISTS duration_ms INTEGER;
ALTER TABLE failed_requests ADD COLUMN IF NOT EXISTS attempt_count INTEGER;
ALTER TABLE failed_requests ADD COLUMN IF NOT EXISTS rate_limit_attempts INTEGER;
ALTER TABLE failed_requests ADD COLUMN IF NOT EXISTS api_key_name TEXT;
ALTER TABLE failed_requests ADD COLUMN IF NOT EXISTS pool_allocation JSONB;
