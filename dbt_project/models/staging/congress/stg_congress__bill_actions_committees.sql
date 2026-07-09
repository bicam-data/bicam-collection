{{ config(tags=["bills", "staging"]) }}

with actions as (
    {{ resolve_child('bill_detail', 'bill_actions', 'bills') }}
),
bill_actions as (
    select
        action_id,
        bill_id
    from {{ ref('stg_congress__bill_actions') }}
),
unnested_actions as (
    select
        actions.response,
        action_item.value as action_json
    from actions
    cross join lateral {{ unnest_array('response', 'actions', 'action_item') }}
),
matched_actions as (
    select
        bill_actions.action_id,
        bill_actions.bill_id,
        unnested_actions.action_json
    from unnested_actions
    join bill_actions
      on bill_actions.action_id = {{ hash_to_uuid("bill_actions.bill_id || '|' || coalesce(unnested_actions.action_json::text, '')") }}
),
unnested_committees as (
    select
        action_id,
        bill_id,
        committee_item.value as committee_json
    from matched_actions
    cross join lateral {{ unnest_array('action_json', 'committees', 'committee_item') }}
)

select
distinct
    action_id,
    bill_id,
    nullif(committee_json #>> '{systemCode}', '') as committee_code,
    nullif(committee_json #>> '{name}', '') as committee_name
from unnested_committees
