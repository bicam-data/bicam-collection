{{ config(
    schema='bicam', materialized='table', tags=['bicam_public', 'amendments'],
    indexes=[
      {'columns': ['action_id'], 'unique': true},
      {'columns': ['amendment_id']},
      {'columns': ['action_date']}
    ]
) }}

select distinct
    action_id::text as action_id,
    amendment_id,
    action_code,
    action_date,
    text,
    action_type,
    source_system,
    source_system_code::text as source_system_code
from {{ ref('stg_congress__amendment_actions') }}
