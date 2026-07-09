{{ config(tags=["members", "staging"]) }}

with legislation_responses as (
    {{ resolve_child('member_detail', 'member_cosponsored_legislation', 'members') }}
),
unnested as (
    select
        raw_id,
        parent_raw_id,
        parent_endpoint_url,
        endpoint_url,
        legislation.value as legislation_json
    from legislation_responses
    cross join lateral {{ unnest_array('response', 'cosponsoredLegislation', 'legislation') }}
)

select
    raw_id,
    parent_raw_id,
    parent_endpoint_url,
    endpoint_url,
    {{ extract_fields('legislation_json', [
        ('amendmentNumber', 'text', 'amendment_number'),
        ('congress', 'integer'),
        ('introducedDate', 'date', 'introduced_date'),
        ('type', 'text'),
        ('latestAction.actionDate', 'date', 'latest_action_date'),
        ('latestAction.text', 'text', 'latest_action_text'),
        ('url', 'text', 'legislation_url')
    ]) }}
from unnested
