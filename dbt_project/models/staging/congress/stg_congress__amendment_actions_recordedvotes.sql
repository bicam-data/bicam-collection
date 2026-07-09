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
unnested_votes as (
    select
        amendment_id,
        action_id,
        vote_item.value as vote_json
    from matched_actions
    cross join lateral {{ unnest_array('action_json', 'recordedVotes', 'vote_item') }}
)

select
distinct
    amendment_id,
    action_id,
    lower({{ json_text('vote_json', 'chamber') }}) as chamber,
    {{ json_int('vote_json', 'congress') }} as congress,
    {{ json_timestamptz('vote_json', 'date') }} as date,
    {{ json_int('vote_json', 'rollNumber') }} as roll_number,
    {{ json_int('vote_json', 'sessionNumber') }} as session,
    {{ json_text('vote_json', 'url') }} as url
from unnested_votes
