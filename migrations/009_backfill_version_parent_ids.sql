-- Keep immutable raw versions aligned with the repaired current-response
-- parent lineage.  Legacy versions were written before parent IDs were
-- consistently propagated; the current raw row is the stable parent record
-- for the endpoint and is safe to copy to those historical versions.
UPDATE raw_response_versions AS version
SET parent_id = response.parent_id
FROM raw_responses AS response
WHERE version.raw_response_id = response.id
  AND version.parent_id IS NULL
  AND response.parent_id IS NOT NULL;
