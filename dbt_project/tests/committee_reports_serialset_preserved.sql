/* Every valid repaired SERIALSET identity must survive canonical deduplication
   in the public root, even when it has no Congress.gov counterpart. */
with serialset_ids as (
    select distinct canonical_report_id as report_id
    from {{ ref('int_committee_reports__govinfo') }}
    where collection_code = 'SERIALSET'
      and canonical_report_id is not null
), public_ids as (
    select report_id
    from {{ ref('committee_reports') }}
)
select serialset_ids.report_id
from serialset_ids
left join public_ids using (report_id)
where public_ids.report_id is null
