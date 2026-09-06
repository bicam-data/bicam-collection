{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'treaties'], indexes=[{'columns': ['action_id'], 'unique': true}, {'columns': ['treaty_id']}]) }}

select distinct on (action_id)
    action_id::text as action_id,
    regexp_replace(lower(treaty_part_id), '[a-z]+$', '') as treaty_id,
    action_code,
    action_date::date as action_date,
    text,
    action_type
from {{ ref('stg_congress__treaty_actions') }}
where action_id is not null and treaty_part_id is not null
order by action_id, action_date desc nulls last, treaty_part_id
