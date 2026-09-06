/* Congress.gov bill-side committeeReports are valid report identity evidence
   even when the standalone committee-report endpoint is absent. */
with bill_side as (
    select distinct report_part_id
    from {{ ref('stg_congress__bill_committeereports') }}
    where report_part_id is not null
), congress_union as (
    select distinct canonical_report_id
    from {{ ref('int_committee_reports__congress') }}
)
select bill_side.report_part_id
from bill_side
left join congress_union
  on congress_union.canonical_report_id = bill_side.report_part_id
where congress_union.canonical_report_id is null
