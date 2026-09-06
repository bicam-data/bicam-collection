/* Resolver-backed nomination hearings must retain immutable source lineage
   before the public hearing-edge reduction. */
with checks as (
    select
        count(*)::bigint as row_count,
        count(source_raw_id)::bigint as raw_id_count,
        count(source_raw_response_version_id)::bigint as version_id_count,
        count(nomination_detail_raw_id)::bigint as parent_count,
        count(source_endpoint_url)::bigint as endpoint_count
    from {{ ref('int_congress_nomination_hearing_observations') }}
)
select 'nomination_hearings'::text as child_kind
from checks
where row_count = 0
   or raw_id_count <> row_count
   or version_id_count <> row_count
   or parent_count <> row_count
   or endpoint_count <> row_count
