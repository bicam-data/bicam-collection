select *
from {{ ref('int_committee_report_candidates') }}
where canonical_report_id is null
