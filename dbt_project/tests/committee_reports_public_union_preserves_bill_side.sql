/* A report observed only through a bill-side committeeReports array must
   still have a public report identity; GovInfo may supply the description. */
select reports.canonical_report_id
from {{ ref('int_committee_reports__congress') }} as reports
left join {{ ref('committee_reports') }} as public_reports
  on public_reports.report_id = reports.canonical_report_id
where public_reports.report_id is null
