{{ config(tags=["nominations", "staging"]) }}

with nominee_responses as (
    {{ resolve_child('nomination_detail', 'nomination_nominee_detail', 'nominations') }}
),
unnested as (
    select
        md5(endpoint_url || '|' || nominee.value::text) as raw_id,
        parent_raw_id,
        parent_endpoint_url,
        endpoint_url,
        nominee.value as nominee_json
    from nominee_responses
    cross join lateral {{ unnest_array('response', 'nominees', 'nominee') }}
)

select
    raw_id,
    parent_raw_id,
    parent_endpoint_url,
    endpoint_url,
    {{ extract_fields('nominee_json', [
        ('firstName', 'text', 'first_name'),
        ('middleName', 'text', 'middle_name'),
        ('lastName', 'text', 'last_name'),
        ('ordinal', 'integer'),
        ('state', 'text')
    ]) }}
from unnested
