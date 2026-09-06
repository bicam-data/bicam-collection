{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'nominations'], indexes=[{'columns': ['nomination_id', 'hearing_id'], 'unique': true}, {'columns': ['hearing_id']}]) }}

select distinct
    {{ bicam_public_nomination_id('nomination_part_id') }} as nomination_id,
    public_ids.public_hearing_id as hearing_id
from {{ ref('stg_congress__nomination_hearings') }} as nomination_hearings
join {{ ref('int_congress_hearing_public_id_map') }} as public_ids
  on public_ids.source_hearing_id = nomination_hearings.hearing_id
join {{ ref('hearings') }} as hearings
  on hearings.hearing_id = public_ids.public_hearing_id
where nomination_part_id is not null
  and public_ids.public_hearing_id is not null
