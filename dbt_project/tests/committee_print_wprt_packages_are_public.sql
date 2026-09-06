with expected as (
    select canonical_print_id as print_id, package_id
    from {{ ref('int_committee_prints__govinfo') }}
    where print_type = 'wprt'
      and is_representative_package
),
actual as (
    select print_id, govinfo_package_id as package_id
    from {{ ref('committee_prints_metadata') }}
)

select expected.print_id, expected.package_id
from expected
left join {{ ref('committee_prints') }} as prints using (print_id)
left join actual using (print_id, package_id)
where prints.print_id is null
   or actual.print_id is null
