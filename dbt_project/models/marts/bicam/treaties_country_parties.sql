{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'treaties'], indexes=[{'columns': ['treaty_id', 'country'], 'unique': true}, {'columns': ['country']}]) }}

select distinct
    regexp_replace(lower(treaty_part_id), '[a-z]+$', '') as treaty_id,
    country
from {{ ref('stg_congress__treaty_countries') }}
where treaty_part_id is not null and country is not null
