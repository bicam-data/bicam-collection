select *
from {{ ref('fct_reconciliation_exceptions') }}
where entity = 'committee_report'
  and missing_source = 'congress'
  and split_part(canonical_id, '-', 3)::integer between 1 and 102
  and exception_status <> 'reviewed'
