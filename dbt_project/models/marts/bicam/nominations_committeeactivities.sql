{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'nominations'], indexes=[{'columns': ['nomination_id', 'committee_code', 'activity_date', 'activity_name'], 'unique': true}, {'columns': ['committee_code']}]) }}

select distinct
    {{ bicam_public_nomination_id('nomination_part_id') }} as nomination_id,
    committee_id as committee_code,
    activity_name,
    activity_date::date as activity_date
from {{ ref('stg_congress__nomination_committeeactivities') }}
where nomination_part_id is not null
  and committee_id is not null
  and activity_date is not null
  and activity_name is not null
