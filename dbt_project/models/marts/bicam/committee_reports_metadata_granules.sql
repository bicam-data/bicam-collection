{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committee_reports'],
    indexes=[
      {'columns': ['report_id', 'govinfo_granule_id'], 'unique': true},
      {'columns': ['govinfo_granule_id']}
    ]
) }}

select distinct
    reports.canonical_report_id::text as report_id,
    granules.package_id::text as govinfo_package_id,
    granules.granule_id::text as govinfo_granule_id
from {{ ref('stg_govinfo__committeereports_granules') }} as granules
join {{ ref('int_committee_reports__govinfo') }} as reports
  on reports.package_id = granules.package_id
 and reports.report_part = granules.report_part
 and reports.is_errata = granules.is_errata
where reports.canonical_report_id is not null
  and granules.package_id is not null
  and granules.granule_id is not null
