select entity
from {{ ref('fct_reconciliation_metrics') }}
where eligible_authoritative_record_count
      <> eligible_authoritative_accepted_count
       + unexcused_unmatched_count
       + unexcused_rejected_count
       + unexcused_ambiguous_count
