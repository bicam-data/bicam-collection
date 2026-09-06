{{ config(tags=["house_votes", "staging"]) }}

with details as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        coalesce(response -> 'houseRollCallVote', response) as vote_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'house-votes'
      and node_type = 'house_vote_detail'
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    (nullif(vote_json #>> '{identifier}', ''))::bigint as vote_identifier,
    (nullif(vote_json #>> '{congress}', ''))::integer as congress,
    (nullif(vote_json #>> '{sessionNumber}', ''))::integer as session_number,
    (nullif(vote_json #>> '{rollCallNumber}', ''))::integer as roll_call_number,
    nullif(vote_json #>> '{legislationType}', '') as legislation_type,
    nullif(vote_json #>> '{legislationNumber}', '') as legislation_number,
    nullif(vote_json #>> '{amendmentType}', '') as amendment_type,
    nullif(vote_json #>> '{amendmentNumber}', '') as amendment_number,
    nullif(vote_json #>> '{amendmentAuthor}', '') as amendment_author,
    nullif(vote_json #>> '{voteType}', '') as vote_type,
    nullif(vote_json #>> '{voteQuestion}', '') as vote_question,
    nullif(vote_json #>> '{result}', '') as result,
    (nullif(vote_json #>> '{startDate}', ''))::timestamptz as started_at,
    (nullif(vote_json #>> '{updateDate}', ''))::timestamptz as updated_at,
    nullif(vote_json #>> '{legislationUrl}', '') as legislation_url,
    nullif(vote_json #>> '{sourceDataURL}', '') as source_data_url,
    nullif(vote_json #>> '{url}', '') as url,
    vote_json -> 'votePartyTotal' as vote_party_total
from details
