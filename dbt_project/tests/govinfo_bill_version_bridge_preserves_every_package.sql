with source_packages as (
    select package_id
    from {{ ref('stg_govinfo__bills_packages') }}
),
bridge_packages as (
    select package_id
    from {{ ref('bridge_govinfo__bill_versions') }}
),
missing_from_bridge as (
    select package_id from source_packages
    except
    select package_id from bridge_packages
),
extra_in_bridge as (
    select package_id from bridge_packages
    except
    select package_id from source_packages
)

select package_id, 'missing_from_bridge' as failure_reason
from missing_from_bridge
union all
select package_id, 'extra_in_bridge' as failure_reason
from extra_in_bridge
