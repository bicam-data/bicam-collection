with public_ids as (
    select print_id from {{ ref('committee_prints') }}
    union all select print_id from {{ ref('committee_prints_associated_bills') }}
    union all select print_id from {{ ref('committee_prints_committees') }}
    union all select print_id from {{ ref('committee_prints_metadata') }}
    union all select print_id from {{ ref('committee_prints_metadata_granules') }}
    union all select print_id from {{ ref('committee_prints_texts') }}
)

select print_id
from public_ids
where print_id is null
   or print_id !~ '^[hsjw]prt[0-9]+-[0-9]+$'
