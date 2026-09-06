-- A subset of legacy targeted child crawls did not write parent_endpoint_url
-- telemetry either.  Their API URL still embeds the owning detail identity;
-- repair only exact, unique detail matches and leave anything unresolved.

CREATE TEMP TABLE _derived_parent_candidates ON COMMIT DROP AS
WITH candidates AS (
    SELECT child.id AS child_id, parent.id AS parent_id
    FROM raw_responses AS child
    JOIN raw_responses AS parent
      ON parent.source = child.source
     AND parent.node_type = CASE
           WHEN child.node_type LIKE 'bill_%' THEN 'bill_detail'
           WHEN child.node_type LIKE 'amendment_%' THEN 'amendment_detail'
           WHEN child.node_type LIKE 'treaty_%' THEN 'treaty_detail'
           WHEN child.node_type LIKE 'nomination_%' THEN 'nomination_detail'
           ELSE NULL
         END
     AND parent.endpoint_url IN (
           CASE
             WHEN child.endpoint_url LIKE '%/bill/%'
               THEN regexp_replace(child.endpoint_url,
                                    '(/bill/[0-9]+/[^/]+/[0-9]+)(/.*)?(\\?.*)?$', '\\1') || '?format=json'
             WHEN child.endpoint_url LIKE '%/amendment/%'
               THEN regexp_replace(child.endpoint_url,
                                    '(/amendment/[0-9]+/[^/]+/[0-9]+)(/.*)?(\\?.*)?$', '\\1') || '?format=json'
             WHEN child.endpoint_url LIKE '%/treaty/%'
               THEN regexp_replace(child.endpoint_url,
                                    '(/treaty/[0-9]+/[0-9]+)(/.*)?(\\?.*)?$', '\\1') || '?format=json'
             WHEN child.endpoint_url LIKE '%/nomination/%'
               THEN regexp_replace(child.endpoint_url,
                                    '(/nomination/[0-9]+/[0-9]+(?:-[0-9]+)?)(/.*)?(\\?.*)?$', '\\1') || '?format=json'
             ELSE NULL
           END,
           CASE
             WHEN child.endpoint_url LIKE '%/bill/%'
               THEN regexp_replace(child.endpoint_url,
                                    '(/bill/[0-9]+/[^/]+/[0-9]+)(/.*)?(\\?.*)?$', '\\1') || '?format=xml'
             WHEN child.endpoint_url LIKE '%/amendment/%'
               THEN regexp_replace(child.endpoint_url,
                                    '(/amendment/[0-9]+/[^/]+/[0-9]+)(/.*)?(\\?.*)?$', '\\1') || '?format=xml'
             WHEN child.endpoint_url LIKE '%/treaty/%'
               THEN regexp_replace(child.endpoint_url,
                                    '(/treaty/[0-9]+/[0-9]+)(/.*)?(\\?.*)?$', '\\1') || '?format=xml'
             WHEN child.endpoint_url LIKE '%/nomination/%'
               THEN regexp_replace(child.endpoint_url,
                                    '(/nomination/[0-9]+/[0-9]+(?:-[0-9]+)?)(/.*)?(\\?.*)?$', '\\1') || '?format=xml'
             ELSE NULL
           END
     )
    WHERE child.parent_id IS NULL
      AND child.source IN ('congress', 'govinfo')
      AND child.category NOT LIKE 'storage-test-%'
      AND child.node_type IN (
          'bill_actions', 'bill_amendments', 'bill_committees',
          'bill_cosponsors', 'bill_relatedbills', 'bill_subjects',
          'bill_summaries', 'bill_text', 'bill_titles',
          'amendment_actions', 'amendment_cosponsors',
          'amendment_amendments_to_amendment', 'amendment_texts',
          'treaty_actions', 'treaty_committees',
          'nomination_actions', 'nomination_committees',
          'nomination_hearings', 'nomination_nominee_detail'
      )
    GROUP BY child.id, parent.id
), unique_candidates AS (
    SELECT child_id, min(parent_id::text)::uuid AS parent_id
    FROM candidates
    GROUP BY child_id
    HAVING count(*) = 1
)
SELECT child_id, parent_id
FROM unique_candidates;

CREATE UNIQUE INDEX _derived_parent_candidates_child_idx
    ON _derived_parent_candidates(child_id);

UPDATE raw_responses AS child
SET parent_id = candidates.parent_id
FROM _derived_parent_candidates AS candidates
WHERE child.id = candidates.child_id
  AND child.parent_id IS NULL;

UPDATE raw_response_versions AS version
SET parent_id = candidates.parent_id
FROM _derived_parent_candidates AS candidates
WHERE version.raw_response_id = candidates.child_id
  AND version.parent_id IS NULL;
