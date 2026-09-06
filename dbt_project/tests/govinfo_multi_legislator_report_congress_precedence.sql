/* A nested member object can carry a stale Congress value.  Candidate matching
   must use the report-level Congress whenever it is available. */
select source_member_key, report_id, congress, report_congress, matching_congress
from {{ ref('int_govinfo_multi_legislator_candidates') }}
where report_congress is not null
  and matching_congress is distinct from report_congress
