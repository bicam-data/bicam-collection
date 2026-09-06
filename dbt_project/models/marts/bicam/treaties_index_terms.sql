{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'treaties'], indexes=[{'columns': ['treaty_id', 'index_term'], 'unique': true}, {'columns': ['index_term']}]) }}

select distinct
    regexp_replace(lower(treaty_part_id), '[a-z]+$', '') as treaty_id,
    index_term
from {{ ref('stg_congress__treaty_indexterms') }}
where treaty_part_id is not null and index_term is not null
