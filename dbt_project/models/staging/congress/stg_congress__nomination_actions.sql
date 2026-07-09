{{ config(tags=["nominations", "staging"]) }}

with action_responses as (
    {{ resolve_child('nomination_detail', 'nomination_actions', 'nominations') }}
),
unnested as (
    select
        substring(parent_endpoint_url from '/nomination/([0-9]+)/')::integer as congress,
        substring(parent_endpoint_url from '/nomination/[0-9]+/([0-9]+)') as nomination_number,
        coalesce(
            substring(parent_endpoint_url from '/nomination/[0-9]+/[0-9]+/([^/?]+)'),
            '00'
        ) as nomination_part,
        action.value as action_json
    from action_responses
    cross join lateral {{ unnest_array('response', 'actions', 'action') }}
)

select
    'pn' || nomination_number || '-' || congress::text as nomination_id,
    'pn' || nomination_number || '-' || nomination_part || '-' || congress::text as nomination_part_id,
    {{ hash_to_uuid(
        "'pn' || " ~ "nomination_number" ~ " || '-' || " ~ "nomination_part" ~ " || '-' || " ~ "congress::text" ~ " || '|' || action_json::text"
    ) }} as action_id,
    {{ json_text('action_json', 'actionCode') }} as action_code,
    {{ json_text('action_json', 'type') }} as action_type,
    {{ json_date('action_json', 'actionDate') }} as action_date,
    {{ json_text('action_json', 'text') }} as text
from unnested
