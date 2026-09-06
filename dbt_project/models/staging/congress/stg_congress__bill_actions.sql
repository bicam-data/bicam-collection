{{ config(tags=["bills", "staging"]) }}

with unnested as (
    select
        source_raw_id as raw_id,
        source_fetched_at as child_fetched_at,
        bill_id,
        action_json,
        source_ordinal as array_ordinal
    from {{ ref('int_congress_bill_action_observations') }}
), extracted as (
select
    raw_id,
    child_fetched_at,
    array_ordinal,
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
), keyed as (
    select
        extracted.*,
        row_number() over (
            partition by action_id
            order by child_fetched_at desc nulls last,
                     raw_id desc,
                     array_ordinal desc
        ) as key_rank,
        action_id is not null and bill_id is not null as key_complete
    from extracted
)

select action_id, bill_id, action_code, action_date, text, action_type,
       source_system, source_system_code, calendar, calendar_number
from keyed
where key_rank = 1 or not key_complete
