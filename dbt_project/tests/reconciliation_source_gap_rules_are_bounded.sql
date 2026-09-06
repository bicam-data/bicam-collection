select *
from {{ ref('reconciliation_source_gap_rules') }}
where congress_min is not null
  and congress_max is not null
  and congress_min > congress_max
