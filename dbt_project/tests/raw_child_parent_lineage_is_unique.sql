/* A legacy targeted crawl can omit raw_responses.parent_id even when the
   fetch ledger records the parent endpoint.  Once a unique parent is
   resolvable, the raw row must carry that link; otherwise resolve_child would
   silently omit its payload from staging.  Ambiguous list-to-detail history
   is intentionally excluded from this assertion. */
{{ config(tags=["operations", "coverage", "quality"]) }}

with candidate_parents as (
    select distinct
        child.raw_id as child_raw_id,
        parent.raw_id as parent_raw_id
    from {{ ref('int_raw_responses__current') }} as child
    join {{ source('raw', 'crawl_run_fetches') }} as fetches
      on fetches.endpoint_url = child.endpoint_url
     and fetches.parent_endpoint_url is not null
    join {{ ref('int_raw_responses__current') }} as parent
      on parent.endpoint_url = fetches.parent_endpoint_url
     and parent.source = child.source
    where child.parent_id is null
      and child.source = 'congress'
      and child.category in (
          'amendments', 'bills', 'committee-meetings', 'committee-prints',
          'committee-reports', 'committees', 'hearings', 'members',
          'nominations', 'treaties', 'congresses'
      )
), unique_parent_children as (
    select child_raw_id
    from candidate_parents
    group by child_raw_id
    having count(*) = 1
)
select candidate_parents.child_raw_id
from candidate_parents
join unique_parent_children using (child_raw_id)
where not exists (
    select 1
    from {{ ref('int_raw_responses__current') }} as child
    where child.raw_id = candidate_parents.child_raw_id
      and child.parent_id = candidate_parents.parent_raw_id
)
