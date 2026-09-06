with source_packages as (
    select package_id
    from {{ ref('stg_govinfo__committee_prints_packages') }}
    where upper(coalesce(collection_code, '')) = 'CPRT'
),
bridge_packages as (
    select package_id
    from {{ ref('bridge_govinfo__committee_print_packages') }}
)
select source_packages.package_id
from source_packages
left join bridge_packages using (package_id)
where bridge_packages.package_id is null
