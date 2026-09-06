{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_prints'], indexes=[{'columns': ['print_id', 'govinfo_granule_id'], 'unique': true}, {'columns': ['govinfo_granule_id'], 'unique': true}]) }}

with packages as (
    select distinct canonical_print_id as print_id, package_id
    from {{ ref('int_committee_prints__govinfo') }}
    where canonical_print_id is not null
      and package_id is not null
      and is_representative_package
), granules as (
    select
        raw.response #>> '{packageId}' as package_id,
        raw.response #>> '{granuleId}' as govinfo_granule_id
    from {{ ref('int_raw_responses__current') }} as raw
    where raw.source = 'govinfo'
      and raw.category = 'govinfo-committee-prints'
      and raw.node_type = 'govinfo_committee_print_granule_detail'
)
select distinct
    packages.print_id,
    packages.package_id as govinfo_package_id,
    granules.govinfo_granule_id
from granules
join packages using (package_id)
where granules.govinfo_granule_id is not null
