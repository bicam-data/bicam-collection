-- The derived child-parent repair may run after legacy versions were written.
-- Copy the now-authoritative current parent to any remaining null version
-- lineage fields; this is idempotent and preserves the version payloads.
UPDATE raw_response_versions AS version
SET parent_id = response.parent_id
FROM raw_responses AS response
WHERE version.raw_response_id = response.id
  AND version.parent_id IS NULL
  AND response.parent_id IS NOT NULL;
