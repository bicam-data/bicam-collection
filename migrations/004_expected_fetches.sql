-- Record endpoints discovered and queued during each crawl run. This keeps
-- expected child-resource coverage distinct from the fetch outcome itself.
CREATE TABLE IF NOT EXISTS crawl_run_expected_fetches (
    id UUID PRIMARY KEY,
    crawl_run_id UUID NOT NULL REFERENCES crawl_runs(id),
    endpoint_url TEXT NOT NULL,
    parent_endpoint_url TEXT,
    node_type TEXT NOT NULL,
    follow_mode TEXT NOT NULL,
    expected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (crawl_run_id, endpoint_url, node_type)
);

CREATE INDEX IF NOT EXISTS idx_expected_fetches_run_id
    ON crawl_run_expected_fetches(crawl_run_id);
CREATE INDEX IF NOT EXISTS idx_expected_fetches_endpoint_url
    ON crawl_run_expected_fetches(endpoint_url);
