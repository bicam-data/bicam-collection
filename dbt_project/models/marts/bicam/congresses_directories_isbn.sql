{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'congresses'],
    indexes=[
      {'columns': ['congress_number', 'govinfo_package_id', 'isbn'], 'unique': true},
      {'columns': ['govinfo_package_id']}
    ]
) }}

/* GovInfo's JSON summary omits the historical ISBN values. Keep the small,
   immutable published ISBN set as bootstrap evidence until MODS ingestion is
   added; live CDIR package metadata remains authoritative for directories.
   Preserve every package-level observation: the published archive contains
   repeated ISBNs for separate directory packages and those package identities
   are part of the child-table grain. */
select
    bootstrap.congress_number::integer as congress_number,
    bootstrap.govinfo_package_id::text as govinfo_package_id,
    bootstrap.isbn::text as isbn
from {{ ref('bootstrap_congresses_directories_isbn') }} as bootstrap
join {{ ref('congresses') }} as congresses
  on congresses.congress_number = bootstrap.congress_number::integer
where bootstrap.isbn is not null
