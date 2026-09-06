select entity
from {{ ref('fct_reconciliation_metrics') }}
where not threshold_present
  and quality_status = 'pass'
