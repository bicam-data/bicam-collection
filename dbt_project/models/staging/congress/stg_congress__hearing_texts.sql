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
        hearing_format.value as format_json
    from detail
    cross join lateral {{ unnest_array('hearing_json', 'formats', 'hearing_format') }}
),
pivoted as (
    select
        hearing_id,
        hearing_part_id,
        max(case when {{ json_text('format_json', 'type') }} = 'Formatted Text'
            then {{ json_text('format_json', 'url') }} end) as formatted_text,
        max(case when {{ json_text('format_json', 'type') }} = 'PDF'
            then {{ json_text('format_json', 'url') }} end) as pdf
    from unnested
    group by 1, 2
)

select
    hearing_id,
    hearing_part_id,
    null::text as raw_text,
    formatted_text,
    pdf
from pivoted
