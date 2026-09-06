{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'nominations'], indexes=[{'columns': ['action_id'], 'unique': true}, {'columns': ['nomination_id']}]) }}

select distinct
    action_id::text as action_id,
    {{ bicam_public_nomination_id('nomination_part_id') }} as nomination_id,
    action_code,
    action_type,
    action_date::date as action_date,
    text
from {{ ref('stg_congress__nomination_actions') }}
where action_id is not null and nomination_part_id is not null
