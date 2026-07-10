select *
from {{ ref('int_bill_candidates') }}
where canonical_bill_id is null
