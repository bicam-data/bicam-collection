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
unnested_votes as (
    select
        action_id,
        bill_id,
        vote_item.value as vote_json
    from matched_actions
    cross join lateral {{ unnest_array('action_json', 'recordedVotes', 'vote_item') }}
)

select
distinct
    action_id,
    bill_id,
    nullif(vote_json #>> '{chamber}', '') as chamber,
    (nullif(vote_json #>> '{congress}', ''))::integer as congress,
    (nullif(vote_json #>> '{date}', ''))::timestamptz as date,
    (nullif(vote_json #>> '{rollNumber}', ''))::integer as roll_number,
    (nullif(vote_json #>> '{sessionNumber}', ''))::integer as session,
    nullif(vote_json #>> '{url}', '') as url
from unnested_votes
