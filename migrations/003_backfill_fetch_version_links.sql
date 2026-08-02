-- Historical fetch rows predate exact version capture. Link them when an
-- endpoint has exactly one retained payload; leave genuinely ambiguous rows
-- null rather than inventing lineage.
WITH singleton_versions AS (
    SELECT raw_response_id, (array_agg(id ORDER BY id))[1] AS raw_response_version_id
    FROM raw_response_versions
    GROUP BY raw_response_id
    HAVING count(*) = 1
)
UPDATE crawl_run_fetches AS crawl_fetch
SET raw_response_version_id = singleton.raw_response_version_id
FROM singleton_versions AS singleton
WHERE crawl_fetch.raw_response_version_id IS NULL
  AND crawl_fetch.raw_response_id = singleton.raw_response_id;
