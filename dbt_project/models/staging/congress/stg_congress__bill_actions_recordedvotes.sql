{{ config(tags=["bills", "staging"]) }}

with unnested_actions as (
    select
        bill_id,
        action_json
    from {{ ref('int_congress_bill_action_observations') }}
),
unnested_votes as (
    select
        bill_id,
        {{ hash_to_uuid("bill_id || '|' || coalesce(action_json::text, '')") }} as action_id,
        vote_item.value as vote_json
    from unnested_actions
    cross join lateral {{ unnest_array('action_json', 'recordedVotes', 'vote_item') }}
),
extracted as (
    select
        action_id,
        bill_id,
        nullif(vote_json #>> '{chamber}', '') as chamber,
        (nullif(vote_json #>> '{congress}', ''))::integer as congress,
        (nullif(vote_json #>> '{date}', ''))::timestamptz as date,
        (nullif(vote_json #>> '{rollNumber}', ''))::integer as roll_number,
        (nullif(vote_json #>> '{sessionNumber}', ''))::integer as session,
        nullif(vote_json #>> '{url}', '') as url
    from unnested_votes
), keyed as (
    select
        extracted.*,
        row_number() over (
            partition by action_id, chamber, congress, session, roll_number
            order by date desc nulls last, url desc nulls last
        ) as key_rank,
        chamber is not null
            and congress is not null
            and session is not null
            and roll_number is not null as key_complete
    from extracted
)

select action_id, bill_id, chamber, congress, date, roll_number, session, url
from keyed
where key_rank = 1 or not key_complete
