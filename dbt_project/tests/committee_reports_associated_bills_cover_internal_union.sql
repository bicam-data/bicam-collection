/* Reciprocal Congress.gov report/bill evidence must not be lost in the
   release-facing edge table.  Source IDs are mapped to canonical report IDs
   before comparison. */
with expected as (
    select distinct reports.canonical_report_id as report_id, edges.left_id as bill_id
    from {{ ref('int_congress_internal_relationship_union') }} as edges
    join {{ ref('int_committee_reports__congress') }} as reports
      on reports.canonical_report_id = edges.right_id
    where edges.relationship_type = 'bill_committee_report'
      and reports.canonical_report_id is not null
), actual as (
    select report_id, bill_id
    from {{ ref('committee_reports_associated_bills') }}
)
select expected.report_id, expected.bill_id
from expected
left join actual
  on actual.report_id = expected.report_id
 and actual.bill_id = expected.bill_id
where actual.report_id is null
