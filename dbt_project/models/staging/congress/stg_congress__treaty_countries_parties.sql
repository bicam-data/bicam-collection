{{ config(tags=["treaties", "staging"]) }}

with detail_responses as (
    select
        id as source_raw_id,
        endpoint_url,
        response
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'treaties'
      and node_type = 'treaty_detail'
),
treaties as (
    select
        source_raw_id,
        endpoint_url,
        treaty_item.value as treaty_json
    from detail_responses
    cross join lateral {{ unnest_array('response', 'treaty', 'treaty_item') }}
),
unnested as (
    select
        source_raw_id as parent_raw_id,
        endpoint_url as parent_endpoint_url,
        country_item.value as country_json
    from treaties
    cross join lateral {{ unnest_array('treaty_json', 'countriesParties', 'country_item') }}
)

select
    parent_raw_id,
    parent_endpoint_url,
    {{ extract_fields('country_json', [
        ('name', 'text')
    ]) }}
from unnested
