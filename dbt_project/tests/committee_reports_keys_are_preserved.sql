/* Repeated committee-side report observations must collapse at the
   committee/report-part grain before the internal relationship bridge uses
   them. */
select
    committee_id,
    report_id,
    report_part_id,
    count(*) as observed_rows
from {{ ref('stg_congress__committee_reports') }}
where committee_id = 'HSAG'
  and report_id = 'hrpt1-118'
  and report_part_id = 'hrpt1-1-118'
group by committee_id, report_id, report_part_id
having count(*) <> 1
