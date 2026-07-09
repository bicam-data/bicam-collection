{{ config(tags=["amendments", "staging"]) }}

with actions as (
    {{ resolve_child('amendment_detail', 'amendment_actions', 'amendments') }}
),
amendment_actions as (
    select
        action_id,
        amendment_id
    from {{ ref('stg_congress__amendment_actions') }}
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
        amendment_actions.action_id,
        amendment_actions.amendment_id,
        unnested_actions.action_json
    from unnested_actions
    join amendment_actions
      on amendment_actions.action_id = {{ hash_to_uuid("amendment_actions.amendment_id || '|' || coalesce(unnested_actions.action_json::text, '')") }}
),
unnested_committees as (
    select
        amendment_id,
        action_id,
        committee_item.value as committee_json
    from matched_actions
    cross join lateral {{ unnest_array('action_json', 'committees', 'committee_item') }}
)

select
distinct
    amendment_id,
    action_id,
    {{ json_text('committee_json', 'systemCode') }} as committee_code,
    {{ json_text('committee_json', 'name') }} as committee_name
from unnested_committees
