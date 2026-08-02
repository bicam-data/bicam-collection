-- Expose current payloads together with the exact immutable payload version.
CREATE OR REPLACE VIEW raw_response_current AS
SELECT
    raw.*,
    version.id AS raw_response_version_id
FROM raw_responses AS raw
LEFT JOIN LATERAL (
    SELECT versions.id
    FROM raw_response_versions AS versions
    WHERE versions.raw_response_id = raw.id
      AND versions.response_hash = raw.response_hash
    ORDER BY versions.last_seen_at DESC, versions.id DESC
    LIMIT 1
) AS version ON TRUE;
