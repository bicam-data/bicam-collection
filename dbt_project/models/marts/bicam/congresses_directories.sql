{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'congresses'],
    indexes=[
      {'columns': ['congress_number', 'title'], 'unique': true},
      {'columns': ['govinfo_package_id'], 'unique': true}
    ]
) }}

select distinct on (directories.congress_number, directories.title)
    directories.congress_number,
    directories.title,
    directories.issued_at,
    directories.package_id as govinfo_package_id,
    directories.su_doc_class_number,
    directories.migrated_doc_id,
    directories.ils_system_id,
    directories.collection_code as govinfo_collection_code,
    directories.government_author1,
    directories.government_author2,
    directories.publisher,
    directories.last_modified::date as last_modified
from {{ ref('stg_govinfo__congressional_directories') }} as directories
join {{ ref('congresses') }} as congresses using (congress_number)
where directories.congress_number is not null
  and directories.title is not null
  and directories.package_id is not null
order by
    directories.congress_number,
    directories.title,
    directories.last_modified desc nulls last,
    directories.package_id

