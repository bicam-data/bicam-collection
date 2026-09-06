/* An authoritative unmatched/rejected record without a reviewed source-gap
   disposition must make the reconciliation quality status fail. */
select entity, unexcused_unmatched_count, unexcused_rejected_count, quality_status
from {{ ref('fct_reconciliation_metrics') }}
where quality_status = 'pass'
  and (unexcused_unmatched_count > 0 or unexcused_rejected_count > 0)
