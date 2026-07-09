-- Preserve every distinct payload observed at a canonical endpoint. The
-- application keeps raw_responses as current state and writes versions first.
CREATE TABLE IF NOT EXISTS raw_response_versions (
    id UUID PRIMARY KEY,
    raw_response_id UUID NOT NULL REFERENCES raw_responses(id),
    endpoint_url TEXT NOT NULL,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    node_type TEXT NOT NULL,
    parent_id UUID REFERENCES raw_responses(id),
    response JSONB NOT NULL,
    response_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_crawl_run_id UUID REFERENCES crawl_runs(id),
    latest_crawl_run_id UUID REFERENCES crawl_runs(id),
    UNIQUE(raw_response_id, response_hash)
);

CREATE INDEX IF NOT EXISTS idx_raw_response_versions_raw_response_id
    ON raw_response_versions(raw_response_id);
CREATE INDEX IF NOT EXISTS idx_raw_response_versions_latest_run
    ON raw_response_versions(latest_crawl_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_response_versions_source_category
    ON raw_response_versions(source, category);

ALTER TABLE crawl_run_fetches
    ADD COLUMN IF NOT EXISTS raw_response_version_id UUID REFERENCES raw_response_versions(id);

INSERT INTO raw_response_versions (
    id, raw_response_id, endpoint_url, source, category, node_type, parent_id,
    response, response_hash, first_seen_at, last_seen_at,
    first_crawl_run_id, latest_crawl_run_id
)
SELECT
    (
        substr(md5(raw.id::text || raw.response_hash), 1, 8) || '-'
        || substr(md5(raw.id::text || raw.response_hash), 9, 4) || '-'
        || substr(md5(raw.id::text || raw.response_hash), 13, 4) || '-'
        || substr(md5(raw.id::text || raw.response_hash), 17, 4) || '-'
        || substr(md5(raw.id::text || raw.response_hash), 21, 12)
    )::uuid,
    raw.id, raw.endpoint_url, raw.source, raw.category, raw.node_type, raw.parent_id,
    raw.response, raw.response_hash, raw.fetched_at, raw.fetched_at,
    raw.latest_crawl_run_id, raw.latest_crawl_run_id
FROM raw_responses AS raw
ON CONFLICT (raw_response_id, response_hash) DO NOTHING;

UPDATE crawl_run_fetches AS crawl_fetch
SET raw_response_version_id = version.id
FROM raw_response_versions AS version
WHERE crawl_fetch.raw_response_version_id IS NULL
  AND crawl_fetch.raw_response_id = version.raw_response_id
  AND crawl_fetch.crawl_run_id = version.latest_crawl_run_id;

