{{ config(tags=["amendments", "staging"]) }}

with unnested_actions as (
    select
        amendment_id,
        action_json
    from {{ ref('int_congress_amendment_action_observations') }}
),
unnested_votes as (
    select
        amendment_id,
        {{ hash_to_uuid("amendment_id || '|' || coalesce(action_json::text, '')") }} as action_id,
        vote_item.value as vote_json
    from unnested_actions
    cross join lateral {{ unnest_array('action_json', 'recordedVotes', 'vote_item') }}
),
extracted as (
    select
        amendment_id,
        action_id,
        lower({{ json_text('vote_json', 'chamber') }}) as chamber,
        {{ json_int('vote_json', 'congress') }} as congress,
        {{ json_timestamptz('vote_json', 'date') }} as date,
        {{ json_int('vote_json', 'rollNumber') }} as roll_number,
        {{ json_int('vote_json', 'sessionNumber') }} as session,
        {{ json_text('vote_json', 'url') }} as url
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

select amendment_id, action_id, chamber, congress, date, roll_number, session, url
from keyed
where key_rank = 1 or not key_complete
