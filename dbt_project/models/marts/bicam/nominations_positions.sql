{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'nominations'], indexes=[{'columns': ['nomination_id', 'ordinal', 'position_title'], 'unique': true}, {'columns': ['nomination_id']}]) }}

select distinct on (nomination_part_id, position_id, position_title)
    {{ bicam_public_nomination_id('nomination_part_id') }} as nomination_id,
    position_id as ordinal,
    position_title,
    organization,
    intro_text,
    nominee_count::integer as nominee_count
from {{ ref('stg_congress__nomination_positions') }}
where nomination_part_id is not null
  and position_id is not null
  and position_title is not null
order by
    nomination_part_id,
    position_id,
    position_title,
    (organization is not null) desc,
    (intro_text is not null) desc,
    nominee_count desc nulls last
