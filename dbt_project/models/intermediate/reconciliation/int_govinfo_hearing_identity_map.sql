{{ config(
    tags=['govinfo_hearings', 'reconciliation', 'intermediate'],
    indexes=[{'columns': ['package_id'], 'unique': true}, {'columns': ['hearing_id']}]
) }}

/* One public identity per exact GovInfo CHRG package. Package suffixes such
   as p1, Op1, O, and v1 are meaningful published variants and remain part of
   the root identity. Cross-source match IDs must not collapse that grain. */
select
    packages.package_id,
    {{ bicam_govinfo_hearing_id('packages.package_id') }} as hearing_id
from {{ ref('stg_govinfo__hearings_packages') }} as packages
where packages.package_id ~* '^CHRG-[0-9]+[hsj]hrg[0-9]+'
