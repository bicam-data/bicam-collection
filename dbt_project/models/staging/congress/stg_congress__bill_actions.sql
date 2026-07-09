{{ config(tags=["bills", "staging"]) }}

with action_responses as (
    {{ resolve_child('bill_detail', 'bill_actions', 'bills') }}
),
bill_detail as (
    select
        raw_id,
        bill_id
    from {{ ref('stg_congress__bill_detail') }}
),
unnested as (
    select
        action_responses.raw_id,
        action_responses.parent_raw_id,
        action_responses.parent_endpoint_url,
        action_responses.endpoint_url,
        bill_detail.bill_id,
        action.value as action_json
    from action_responses
    join bill_detail
      on action_responses.parent_raw_id = bill_detail.raw_id
    cross join lateral {{ unnest_array('response', 'actions', 'action') }}
)

select
distinct
    {{ hash_to_uuid("bill_id || '|' || coalesce(action_json::text, '')") }} as action_id,
    bill_id,
    nullif(action_json #>> '{actionCode}', '') as action_code,
    (nullif(action_json #>> '{actionDate}', ''))::date as action_date,
    nullif(action_json #>> '{text}', '') as text,
    nullif(action_json #>> '{type}', '') as action_type,
    nullif(action_json #>> '{sourceSystem,name}', '') as source_system,
    nullif(action_json #>> '{sourceSystem,code}', '') as source_system_code,
    nullif(action_json #>> '{calendarNumber,calendar}', '') as calendar,
    nullif(action_json #>> '{calendarNumber,number}', '') as calendar_number
from unnested
