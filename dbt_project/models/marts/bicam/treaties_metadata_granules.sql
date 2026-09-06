{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'treaties'], indexes=[{'columns': ['treaty_id', 'govinfo_granule_id'], 'unique': true}]) }}

select
    granules.treaty_id::text as treaty_id,
    granules.package_id::text as govinfo_package_id,
    granules.granule_id::text as govinfo_granule_id
from {{ ref('stg_govinfo__treaty_documents_granules') }} as granules
join {{ ref('treaties') }} as treaties using (treaty_id)
