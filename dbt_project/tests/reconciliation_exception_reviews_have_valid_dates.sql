select *
from {{ ref('reconciliation_exception_reviews') }}
where reviewed_at is not null
  and expires_at is not null
  and expires_at < reviewed_at
