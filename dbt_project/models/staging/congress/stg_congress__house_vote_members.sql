{{ config(tags=["house_votes", "staging"]) }}

with member_responses as (
    {{ resolve_child('house_vote_detail', 'house_vote_members', 'house-votes') }}
),
vote_details as (
    select raw_id, vote_identifier
    from {{ ref('stg_congress__house_vote_detail') }}
),
unnested as (
    select
        member_responses.raw_id,
        member_responses.parent_raw_id,
        vote_details.vote_identifier,
        member.value as member_json
    from member_responses
    join vote_details on vote_details.raw_id = member_responses.parent_raw_id
    cross join lateral jsonb_array_elements(
        coalesce(
            member_responses.response -> 'results',
            member_responses.response #> '{houseRollCallVoteMemberVotes,results}',
            '[]'::jsonb
        )
    ) as member(value)
)

select
    raw_id,
    parent_raw_id,
    vote_identifier,
    coalesce(
        nullif(member_json #>> '{bioguideID}', ''),
        nullif(member_json #>> '{bioguideId}', '')
    ) as bioguide_id,
    nullif(member_json #>> '{firstName}', '') as first_name,
    nullif(member_json #>> '{lastName}', '') as last_name,
    nullif(member_json #>> '{voteCast}', '') as vote_cast,
    nullif(member_json #>> '{voteParty}', '') as vote_party,
    nullif(member_json #>> '{voteState}', '') as vote_state
from unnested
