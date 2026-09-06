{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'treaties'], indexes=[{'columns': ['treaty_id'], 'unique': true}]) }}

select
    packages.treaty_id::text as treaty_id,
    packages.package_id::text as govinfo_package_id,
    packages.su_doc_class_number::text as su_doc_class_number,
    packages.migrated_doc_id::text as migrated_doc_id,
    packages.collection_code::text as govinfo_collection_code
from {{ ref('stg_govinfo__treaty_documents_packages') }} as packages
join {{ ref('treaties') }} as treaties using (treaty_id)
