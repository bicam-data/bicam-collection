{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committee_reports'],
    indexes=[
      {'columns': ['report_id'], 'unique': true},
      {'columns': ['govinfo_package_id']}
    ]
) }}

with ranked_packages as (
    select
        reports.canonical_report_id as report_id,
        reports.package_id as govinfo_package_id,
        packages.su_doc_class_number,
        packages.migrated_doc_id,
        packages.collection_code as govinfo_collection_code,
        row_number() over (
            partition by reports.canonical_report_id
            order by (reports.collection_code = 'CRPT') desc,
                     reports.source_updated_at desc nulls last,
                     reports.package_id desc
        ) as _rn
    from {{ ref('int_committee_reports__govinfo') }} as reports
    join {{ ref('stg_govinfo__committeereports_packages') }} as packages
      on packages.package_id = reports.package_id
    where reports.canonical_report_id is not null
      and reports.package_id is not null
)
select
    report_id::text as report_id,
    govinfo_package_id::text as govinfo_package_id,
    su_doc_class_number::text as su_doc_class_number,
    migrated_doc_id::text as migrated_doc_id,
    govinfo_collection_code::text as govinfo_collection_code
from ranked_packages
where _rn = 1
