-- Legacy bill-cosponsor pages were sometimes harvested as independent
-- targeted URLs, so no parent URL was recorded.  Recover their owning bill
-- detail by the exact path embedded in the child URL.  A missing or ambiguous
-- detail is intentionally left unresolved.

CREATE TEMP TABLE _bill_cosponsor_parent_candidates ON COMMIT DROP AS
WITH candidates AS (
    SELECT child.id AS child_id, parent.id AS parent_id
    FROM raw_responses AS child
    CROSS JOIN LATERAL (
        SELECT regexp_replace(
            child.endpoint_url,
            '(/bill/[0-9]+/[^/]+/[0-9]+)(/.*)?(\\?.*)?$',
            '\\1'
        ) AS bill_path
    ) AS path
    JOIN raw_responses AS parent
      ON parent.source = 'congress'
     AND parent.node_type = 'bill_detail'
     AND parent.endpoint_url = path.bill_path || '?format=json'
    WHERE child.source = 'congress'
      AND child.category = 'bills'
      AND child.node_type = 'bill_cosponsors'
      AND child.parent_id IS NULL
), unique_candidates AS (
    SELECT child_id, min(parent_id::text)::uuid AS parent_id
    FROM candidates
    GROUP BY child_id
    HAVING count(*) = 1
)
SELECT child_id, parent_id
FROM unique_candidates;

CREATE UNIQUE INDEX _bill_cosponsor_parent_candidates_child_idx
    ON _bill_cosponsor_parent_candidates(child_id);

UPDATE raw_responses AS child
SET parent_id = candidates.parent_id
FROM _bill_cosponsor_parent_candidates AS candidates
WHERE child.id = candidates.child_id
  AND child.parent_id IS NULL;

UPDATE raw_response_versions AS version
SET parent_id = candidates.parent_id
FROM _bill_cosponsor_parent_candidates AS candidates
WHERE version.raw_response_id = candidates.child_id
  AND version.parent_id IS NULL;
