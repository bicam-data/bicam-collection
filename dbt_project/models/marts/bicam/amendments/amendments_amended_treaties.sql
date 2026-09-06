{{ config(
    schema='bicam', materialized='table', tags=['bicam_public', 'amendments', 'treaties'],
    indexes=[
      {'columns': ['amendment_id', 'treaty_id'], 'unique': true},
      {'columns': ['treaty_id']}
    ]
) }}

select distinct
    amendment_id,
    regexp_replace(lower(treaty_id), '[a-z]+$', '') as treaty_id
from {{ ref('stg_congress__amendment_amended_treaties') }}
where amendment_id is not null and treaty_id is not null
