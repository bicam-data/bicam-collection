{{ config(tags=["treaties", "staging"]) }}

with action_responses as (
    {{ resolve_child('treaty_detail', 'treaty_actions', 'treaties') }}
),
unnested as (
    select
        substring(endpoint_url from '/treaty/([0-9]+)/')::integer as congress,
        substring(endpoint_url from '/treaty/[0-9]+/([0-9]+)')::integer as treaty_number,
        substring(endpoint_url from '/treaty/[0-9]+/[0-9]+/([^/?]+)/actions') as suffix,
        action_item.value as action_json
    from action_responses
    cross join lateral {{ unnest_array('response', 'actions', 'action_item') }}
)

select
    'td'
    || congress::text
    || '-'
    || treaty_number::text as treaty_id,
    'td'
    || congress::text
    || '-'
    || treaty_number::text
    || coalesce(suffix, '') as treaty_part_id,
    {{ hash_to_uuid(
        "'td' || congress::text || '-' || treaty_number::text || coalesce(suffix, '') || '|' || action_json::text"
    ) }} as action_id,
    {{ json_text('action_json', 'actionCode') }} as action_code,
    {{ json_date('action_json', 'actionDate') }} as action_date,
    {{ json_text('action_json', 'text') }} as text,
    {{ json_text('action_json', 'type') }} as action_type,
    {{ json_text('action_json', 'committee.systemCode') }} as committee_id,
    {{ json_text('action_json', 'committee.name') }} as committee_name
from unnested
