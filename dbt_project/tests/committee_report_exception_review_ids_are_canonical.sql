select *
from {{ ref('reconciliation_exception_reviews') }}
where entity = 'committee_report'
  and canonical_id !~ '^[a-z]+[0-9]+-[0-9]+e?-[0-9]+$'
