{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'treaties'], indexes=[{'columns': ['treaty_id', 'title_type', 'title'], 'unique': true}, {'columns': ['treaty_id']}]) }}

select distinct
    regexp_replace(lower(treaty_part_id), '[a-z]+$', '') as treaty_id,
    title,
    title_type
from {{ ref('stg_congress__treaty_titles') }}
where treaty_part_id is not null and title is not null and title_type is not null
