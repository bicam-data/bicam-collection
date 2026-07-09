{{ config(tags=["amendments", "staging"]) }}

with action_responses as (
    {{ resolve_child('amendment_detail', 'amendment_actions', 'amendments') }}
),
amendment_detail as (
    select
        raw_id,
        amendment_id
    from {{ ref('stg_congress__amendment_detail') }}
),
unnested as (
    select
        action_responses.raw_id,
        action_responses.parent_raw_id,
        amendment_detail.amendment_id,
        action.value as action_json
    from action_responses
    join amendment_detail
      on action_responses.parent_raw_id = amendment_detail.raw_id
    cross join lateral {{ unnest_array('response', 'actions', 'action') }}
)

select
distinct
    {{ hash_to_uuid("amendment_id || '|' || coalesce(action_json::text, '')") }} as action_id,
    amendment_id,
    {{ json_text('action_json', 'actionCode') }} as action_code,
    {{ json_date('action_json', 'actionDate') }} as action_date,
    {{ json_text('action_json', 'text') }} as text,
    {{ json_text('action_json', 'type') }} as action_type,
    {{ json_text('action_json', 'sourceSystem.name') }} as source_system,
    {{ json_int('action_json', 'sourceSystem.code') }} as source_system_code
from unnested
