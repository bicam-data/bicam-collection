/* Bill-side and standalone Congress observations may share one canonical
   report. Candidate reduction must keep one cross-source pair per identity. */
select canonical_report_id
from {{ ref('int_committee_report_candidates') }}
group by canonical_report_id
having count(*) > 1
