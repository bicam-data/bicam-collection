{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'hearings'],
    indexes=[
      {'columns': ['hearing_id', 'witness'], 'unique': true},
      {'columns': ['hearing_id']}
    ]
) }}

with array_witnesses as (
    select
        granules.hearing_id,
        nullif(trim(witness.value #>> '{}'), '') as witness
    from {{ ref('int_govinfo_hearing_granules') }} as granules
    cross join lateral jsonb_array_elements(granules.granule_json -> 'witnesses') as witness(value)
    where jsonb_typeof(granules.granule_json -> 'witnesses') = 'array'
),
scalar_witnesses as (
    select
        hearing_id,
        nullif(trim(granule_json #>> '{witnesses}'), '') as witness
    from {{ ref('int_govinfo_hearing_granules') }}
    where jsonb_typeof(granule_json -> 'witnesses') = 'string'
),
all_witnesses as (
    select * from array_witnesses
    union all
    select * from scalar_witnesses
)

select distinct all_witnesses.hearing_id, all_witnesses.witness
from all_witnesses
join {{ ref('hearings') }} as hearings using (hearing_id)
where all_witnesses.hearing_id is not null
  and all_witnesses.witness is not null
