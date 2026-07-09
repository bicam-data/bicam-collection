{{ config(tags=["treaties", "staging"]) }}

with detail_responses as (
    select
        response
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'treaties'
      and node_type = 'treaty_detail'
),
treaties as (
    select
        treaty.value as treaty_json
    from detail_responses
    cross join lateral {{ unnest_array('response', 'treaty', 'treaty') }}
),
unnested as (
    select
        'td' || {{ json_int('treaty_json', 'congressReceived') }}::text || '-' || {{ json_text('treaty_json', 'number') }} as treaty_id,
        'td' || {{ json_int('treaty_json', 'congressReceived') }}::text || '-' || {{ json_text('treaty_json', 'number') }} || coalesce({{ json_text('treaty_json', 'suffix') }}, '') as treaty_part_id,
        country.value as country_json
    from treaties
    cross join lateral {{ unnest_array('treaty_json', 'countriesParties', 'country') }}
)

select
    treaty_id,
    treaty_part_id,
    {{ json_text('country_json', 'name') }} as country
from unnested
