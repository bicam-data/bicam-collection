{{ config(tags=["nominations", "staging"]) }}

with detail as (
    select
        response -> 'nomination' as nomination_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'nominations'
      and node_type = 'nomination_detail'
),
unnested as (
    select
        'pn'
        || {{ json_text('nomination_json', 'number') }}
        || '-'
        || {{ json_int('nomination_json', 'congress') }}::text as nomination_id,
        'pn'
        || {{ json_text('nomination_json', 'number') }}
        || '-'
        || coalesce({{ json_text('nomination_json', 'partNumber') }}, '00')
        || '-'
        || {{ json_int('nomination_json', 'congress') }}::text as nomination_part_id,
        nominee.value as nominee_json
    from detail
    cross join lateral {{ unnest_array('nomination_json', 'nominees', 'nominee') }}
)

select
    nomination_id,
    nomination_part_id,
    {{ json_int('nominee_json', 'ordinal') }} as position_id,
    {{ json_text('nominee_json', 'positionTitle') }} as position_title,
    {{ json_text('nominee_json', 'organization') }} as organization,
    {{ json_text('nominee_json', 'division') }} as division,
    {{ json_text('nominee_json', 'introText') }} as intro_text,
    coalesce({{ json_int('nominee_json', 'nomineeCount') }}, 0) as nominee_count
from unnested
