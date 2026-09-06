{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'hearings'], indexes=[{'columns': ['hearing_id', 'govinfo_granule_id'], 'unique': true}, {'columns': ['hearing_id']}]) }}

select distinct
    hearing_id,
    package_id as govinfo_package_id,
    granule_id as govinfo_granule_id
from {{ ref('int_govinfo_hearing_granules') }}
where hearing_id is not null
  and package_id is not null
  and granule_id is not null

