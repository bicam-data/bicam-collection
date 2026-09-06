/* Resolver-backed amendment action occurrences must retain source lineage. */
with checks as (
    select
        count(*)::bigint as row_count,
        count(source_raw_id)::bigint as raw_id_count,
        count(source_endpoint_url)::bigint as endpoint_count
    from {{ ref('int_congress_amendment_action_observations') }}
    where amendment_id = 'samdt1-118'
)
select 'amendment_actions'::text as child_kind
from checks
where row_count = 0
   or raw_id_count <> row_count
   or endpoint_count <> row_count
