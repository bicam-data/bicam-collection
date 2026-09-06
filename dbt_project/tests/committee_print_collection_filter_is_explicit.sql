select package_id, collection_code
from {{ ref('bridge_govinfo__committee_print_packages') }}
where upper(coalesce(collection_code, '')) <> 'CPRT'
