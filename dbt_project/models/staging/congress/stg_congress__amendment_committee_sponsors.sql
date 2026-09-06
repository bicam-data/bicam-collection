{{ config(tags=["amendments", "staging"]) }}

with detail as (
    select
        raw_id as parent_raw_id,
        endpoint_url as parent_endpoint_url,
        response -> 'amendment' as amendment_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'amendments'
      and node_type = 'amendment_detail'
),
unnested as (
    select
        parent_raw_id,
        parent_endpoint_url,
        sponsor.value as sponsor_json
    from detail
    cross join lateral {{ unnest_array('amendment_json', 'sponsors', 'sponsor') }}
)

select
    parent_raw_id,
    parent_endpoint_url,
    {{ extract_fields('sponsor_json', [
        ('fullName', 'text', 'full_name'),
        ('firstName', 'text', 'first_name'),
        ('middleName', 'text', 'middle_name'),
        ('lastName', 'text', 'last_name'),
        ('party', 'text'),
        ('state', 'text'),
        ('district', 'integer'),
        ('url', 'text', 'committee_url')
    ]) }}
from unnested
where nullif(sponsor_json #>> '{bioguideId}', '') is null
