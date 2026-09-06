{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'nominations'], indexes=[{'columns': ['action_id', 'committee_code'], 'unique': true}, {'columns': ['nomination_id']}]) }}

select distinct
    action_id::text as action_id,
    {{ bicam_public_nomination_id('nomination_part_id') }} as nomination_id,
    committee_id as committee_code
from {{ ref('stg_congress__nomination_actions_committees') }}
where action_id is not null
  and nomination_part_id is not null
  and committee_id is not null
