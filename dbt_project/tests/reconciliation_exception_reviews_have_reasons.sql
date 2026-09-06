select *
from {{ ref('reconciliation_exception_reviews') }}
where disposition <> 'pending'
  and nullif(trim(reason), '') is null
