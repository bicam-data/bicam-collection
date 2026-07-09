{{ config(tags=["hearings", "staging"]) }}

with detail as (
    select
        response -> 'hearing' as hearing_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'hearings'
      and node_type = 'hearing_detail'
),
unnested as (
    select
        lower(left(lower({{ json_text('hearing_json', 'chamber') }}), 1))
        || 'hrg'
        || {{ json_bigint('hearing_json', 'jacketNumber') }}::text
        || '-'
        || {{ json_int('hearing_json', 'congress') }}::text as hearing_id,
        lower(left(lower({{ json_text('hearing_json', 'chamber') }}), 1))
        || 'hrg'
        || {{ json_bigint('hearing_json', 'jacketNumber') }}::text
        || '-'
        || coalesce({{ json_text('hearing_json', 'part') }}, '0')
        || '-'
        || {{ json_int('hearing_json', 'congress') }}::text as hearing_part_id,
        hearing_date.value as date_json
    from detail
    cross join lateral {{ unnest_array('hearing_json', 'dates', 'hearing_date') }}
)

select
    hearing_id,
    hearing_part_id,
    {{ json_date('date_json', 'date') }} as hearing_date
from unnested
