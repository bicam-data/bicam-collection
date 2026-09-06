{{ config(tags=["treaties", "staging"]) }}

with detail_responses as (
    select
        raw_id as source_raw_id,
        endpoint_url,
        response
    from {{ ref('int_raw_responses__current') }}
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
        term_item.value as term_json
    from treaties
    cross join lateral {{ unnest_array('treaty_json', 'indexTerms', 'term_item') }}
)

select
    parent_raw_id,
    parent_endpoint_url,
    {{ extract_fields('term_json', [
        ('name', 'text')
    ]) }}
from unnested
