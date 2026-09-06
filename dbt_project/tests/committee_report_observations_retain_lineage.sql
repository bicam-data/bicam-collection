/* Resolver-backed committee-report memberships must retain source lineage. */
with checks as (
    select
        count(*)::bigint as row_count,
        count(source_raw_id)::bigint as raw_id_count,
        count(source_endpoint_url)::bigint as endpoint_count
    from {{ ref('int_congress_committee_report_observations') }}
)
select 'committee_reports'::text as child_kind
from checks
where row_count = 0
   or raw_id_count <> row_count
   or endpoint_count <> row_count
