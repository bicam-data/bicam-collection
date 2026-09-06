/* Any incomplete-generation row must be a complete, actionable queue input. */
select
    child_collection,
    canonical_id,
    endpoint_url,
    decision_rule
from {{ ref('fct_child_state_gaps') }}
where entity <> 'child_state'
   or missing_source <> 'congress'
   or canonical_id is null
   or endpoint_url is null
   or decision_rule <> 'incomplete_child_generation'
   or review_disposition <> 'needs_source_refresh'
