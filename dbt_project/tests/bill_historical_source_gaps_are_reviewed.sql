select *
from {{ ref('fct_reconciliation_exceptions') }}
where entity = 'bill'
  and missing_source = 'govinfo'
  and split_part(canonical_id, '-', 2)::integer between 6 and 102
  and exception_status <> 'reviewed'
