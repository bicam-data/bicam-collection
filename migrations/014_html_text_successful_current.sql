-- Keep the latest successful HTML extraction available when a later retry
-- records a transient HTTP or network failure.  html_text_current remains the
-- latest-attempt view used by progress and retry logic; this view is the
-- stable content surface used by public text marts.
CREATE OR REPLACE VIEW html_text_successful_current AS
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
WHERE extraction_status = 'success'
ORDER BY url, fetched_at DESC, id DESC;

CREATE INDEX IF NOT EXISTS idx_html_text_versions_success_url_fetched
    ON html_text_versions(url, fetched_at DESC)
    WHERE extraction_status = 'success';
