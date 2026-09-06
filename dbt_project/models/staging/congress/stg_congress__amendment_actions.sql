{{ config(tags=["amendments", "staging"]) }}

with unnested as (
    select
        source_raw_id as raw_id,
        source_fetched_at as child_fetched_at,
        amendment_id,
        action_json,
        source_ordinal as array_ordinal
    from {{ ref('int_congress_amendment_action_observations') }}
), extracted as (
select
    raw_id,
    child_fetched_at,
    array_ordinal,
    {{ hash_to_uuid("amendment_id || '|' || coalesce(action_json::text, '')") }} as action_id,
    amendment_id,
    {{ json_text('action_json', 'actionCode') }} as action_code,
    {{ json_date('action_json', 'actionDate') }} as action_date,
    {{ json_text('action_json', 'text') }} as text,
    {{ json_text('action_json', 'type') }} as action_type,
    {{ json_text('action_json', 'sourceSystem.name') }} as source_system,
    {{ json_int('action_json', 'sourceSystem.code') }} as source_system_code
from unnested
), keyed as (
    select
        extracted.*,
        row_number() over (
            partition by action_id
            order by child_fetched_at desc nulls last,
                     raw_id desc,
                     array_ordinal desc
        ) as key_rank,
        action_id is not null and amendment_id is not null as key_complete
    from extracted
)

select action_id, amendment_id, action_code, action_date, text, action_type,
       source_system, source_system_code
from keyed
where key_rank = 1 or not key_complete
