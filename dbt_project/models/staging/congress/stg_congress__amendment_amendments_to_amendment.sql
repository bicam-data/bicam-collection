{{ config(tags=["amendments", "staging"]) }}

with amendment_responses as (
    {{ resolve_child('amendment_detail', 'amendment_amendments_to_amendment', 'amendments') }}
),
unnested as (
    select
        raw_id,
        parent_raw_id,
        parent_endpoint_url,
        endpoint_url,
        amendment.value as amendment_json
    from amendment_responses
    cross join lateral {{ unnest_array('response', 'amendments', 'amendment') }}
)

select
    raw_id,
    parent_raw_id,
    parent_endpoint_url,
    endpoint_url,
    {{ extract_fields('amendment_json', [
        ('congress', 'integer'),
        ('number', 'text', 'amendment_number'),
        ('type', 'text', 'amendment_type'),
        ('updateDate', 'timestamptz', 'update_date'),
        ('url', 'text', 'amendment_url')
    ]) }}
from unnested
