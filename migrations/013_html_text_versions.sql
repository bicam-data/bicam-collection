-- Append-only HTML fetch/extraction evidence for Congress.gov text links.
-- PDF and XML renditions are intentionally not represented here.
CREATE TABLE IF NOT EXISTS html_text_versions (
    id UUID PRIMARY KEY,
    url TEXT NOT NULL,
    response_hash TEXT NOT NULL,
    content_type TEXT,
    http_status INTEGER,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    extraction_status TEXT NOT NULL,
    text_content TEXT,
    error TEXT,
    UNIQUE(url, response_hash)
);

CREATE INDEX IF NOT EXISTS idx_html_text_versions_url_fetched
    ON html_text_versions(url, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_html_text_versions_status
    ON html_text_versions(extraction_status);

CREATE OR REPLACE VIEW html_text_current AS
SELECT DISTINCT ON (url)
    id,
    url,
    response_hash,
    content_type,
    http_status,
    fetched_at,
    extraction_status,
    text_content,
    error
FROM html_text_versions
ORDER BY url, fetched_at DESC, id DESC;
