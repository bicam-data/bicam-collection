/* Every parseable GovInfo CHRG package, including meaningful suffix variants,
   must have its own public root and exact metadata row. */
with expected as (
    select
        package_id,
        {{ bicam_govinfo_hearing_id('package_id') }} as hearing_id
    from {{ ref('stg_govinfo__hearings_packages') }}
    where package_id ~* '^CHRG-[0-9]+[hsj]hrg[0-9]+'
), failures as (
    select expected.package_id, expected.hearing_id
    from expected
    left join {{ ref('hearings') }} as hearings using (hearing_id)
    left join {{ ref('hearings_metadata') }} as metadata
      on metadata.hearing_id = expected.hearing_id
     and metadata.govinfo_package_id = expected.package_id
    where hearings.hearing_id is null
       or metadata.hearing_id is null
)
select * from failures
