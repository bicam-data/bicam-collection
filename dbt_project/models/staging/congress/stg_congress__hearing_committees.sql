{{ config(tags=["hearings", "staging"]) }}

with detail as (
    select
        response -> 'hearing' as hearing_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'hearings'
      and node_type = 'hearing_detail'
),
unnested as (
    select
        lower(left({{ congress_hearing_canonical_chamber('hearing_json') }}, 1))
        || 'hrg'
        || {{ json_bigint('hearing_json', 'jacketNumber') }}::text
        || '-'
        || {{ json_int('hearing_json', 'congress') }}::text as hearing_id,
        lower(left({{ congress_hearing_canonical_chamber('hearing_json') }}, 1))
        || 'hrg'
        || {{ json_bigint('hearing_json', 'jacketNumber') }}::text
        || '-'
        || coalesce({{ json_text('hearing_json', 'part') }}, '0')
        || '-'
        || {{ json_int('hearing_json', 'congress') }}::text as hearing_part_id,
        committee.value as committee_json
    from detail
    cross join lateral {{ unnest_array('hearing_json', 'committees', 'committee') }}
)

select
    hearing_id,
    hearing_part_id,
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    {{ json_text('committee_json', 'name') }} as committee_name
from unnested
