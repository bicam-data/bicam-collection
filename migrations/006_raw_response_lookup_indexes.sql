-- Keep destructive test cleanup and targeted version lookups proportional to
-- the raw archive.  The foreign-key check on deleting a response version also
-- uses the second index; without it PostgreSQL scans the entire fetch ledger.
CREATE INDEX IF NOT EXISTS idx_raw_response_versions_endpoint_url
    ON raw_response_versions(endpoint_url);

CREATE INDEX IF NOT EXISTS idx_crawl_run_fetches_raw_response_version_id
    ON crawl_run_fetches(raw_response_version_id);
