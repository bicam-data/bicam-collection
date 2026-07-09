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
        related_doc_item.value as related_doc_json
    from treaties
    cross join lateral {{ unnest_array('treaty_json', 'relatedDocs', 'related_doc_item') }}
)

select
    parent_raw_id,
    parent_endpoint_url,
    coalesce(
        nullif(related_doc_json #>> '{name}', ''),
        nullif(related_doc_json #>> '{citation}', '')
    ) as name,
    {{ extract_fields('related_doc_json', [
        ('url', 'text', 'related_doc_url')
    ]) }}
from unnested
