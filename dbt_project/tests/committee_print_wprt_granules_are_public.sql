with wprt_packages as (
    select canonical_print_id as print_id, package_id
    from {{ ref('int_committee_prints__govinfo') }}
    where print_type = 'wprt'
      and is_representative_package
),
expected as (
    select
        packages.print_id,
        packages.package_id,
        raw.response #>> '{granuleId}' as granule_id
    from wprt_packages as packages
    join {{ ref('int_raw_responses__current') }} as raw
      on raw.response #>> '{packageId}' = packages.package_id
     and raw.source = 'govinfo'
     and raw.category = 'govinfo-committee-prints'
     and raw.node_type = 'govinfo_committee_print_granule_detail'
),
actual as (
    select
        print_id,
        govinfo_package_id as package_id,
        govinfo_granule_id as granule_id
    from {{ ref('committee_prints_metadata_granules') }}
)

select expected.print_id, expected.package_id, expected.granule_id
from expected
left join actual using (print_id, package_id, granule_id)
where expected.granule_id is not null
  and actual.granule_id is null
