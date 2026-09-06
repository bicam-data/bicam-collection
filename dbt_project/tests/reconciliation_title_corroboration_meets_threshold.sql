/* Bill identity matches are deterministic, but title corroboration is an
   independent quality signal.  The metric model applies the configured
   threshold only to entities with comparable titles (currently bills). */
select
    entity,
    title_comparable_accepted_count,
    title_agreement_count,
    title_agreement_rate,
    min_title_agreement_rate
from {{ ref('fct_reconciliation_metrics') }}
where min_title_agreement_rate > 0
  and (
      title_comparable_accepted_count = 0
      or title_agreement_count > title_comparable_accepted_count
      or title_agreement_rate < min_title_agreement_rate
  )
