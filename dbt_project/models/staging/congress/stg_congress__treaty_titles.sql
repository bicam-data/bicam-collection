{{ config(tags=["treaties", "staging"]) }}

with detail_responses as (
    select
        id as raw_id,
        response
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'treaties'
      and node_type = 'treaty_detail'
),
treaties as (
    select
        raw_id,
        treaty.value as treaty_json
    from detail_responses
    cross join lateral {{ unnest_array('response', 'treaty', 'treaty') }}
),
unnested as (
    select
        raw_id as parent_raw_id,
        'td' || {{ json_int('treaty_json', 'congressReceived') }}::text || '-' || {{ json_text('treaty_json', 'number') }} as treaty_id,
        'td' || {{ json_int('treaty_json', 'congressReceived') }}::text || '-' || {{ json_text('treaty_json', 'number') }} || coalesce({{ json_text('treaty_json', 'suffix') }}, '') as treaty_part_id,
        title.value as title_json
    from treaties
    cross join lateral {{ unnest_array('treaty_json', 'titles', 'title') }}
)

select
    parent_raw_id,
    treaty_id,
    treaty_part_id,
    {{ json_text('title_json', 'title') }} as title,
    {{ json_text('title_json', 'titleType') }} as title_type
from unnested
