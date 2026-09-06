/* Resolver-backed bill child observations must retain source lineage. */
with checks as (
    select
        'subject'::text as child_kind,
        count(*)::bigint as row_count,
        count(source_raw_id)::bigint as raw_id_count,
        count(source_endpoint_url)::bigint as endpoint_count
    from {{ ref('int_congress_bill_subject_observations') }}
    where bill_id = 'hr1-118'

    union all

    select
        'summary'::text,
        count(*)::bigint,
        count(source_raw_id)::bigint,
        count(source_endpoint_url)::bigint
    from {{ ref('int_congress_bill_summary_observations') }}
    where bill_id = 'hr1-118'
)
select child_kind
from checks
where row_count = 0
   or raw_id_count <> row_count
   or endpoint_count <> row_count
