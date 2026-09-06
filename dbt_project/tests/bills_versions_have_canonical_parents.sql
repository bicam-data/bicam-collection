select v.govinfo_package_id, v.bill_id
from {{ ref('bills_versions') }} as v
left join {{ ref('bills') }} as b using (bill_id)
where b.bill_id is null
