select
    canonical_bill_id,
    count(*) filter (where is_representative_package) as representative_count
from {{ ref('bridge_govinfo__bill_versions') }}
group by canonical_bill_id
having count(*) filter (where is_representative_package) <> 1
