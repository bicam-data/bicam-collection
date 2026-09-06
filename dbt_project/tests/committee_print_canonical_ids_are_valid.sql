select package_id, canonical_print_id
from {{ ref('bridge_govinfo__committee_print_packages') }}
where canonical_print_id is not null
  and canonical_print_id !~ '^[hsjw]prt[0-9]+-[0-9]+$'
