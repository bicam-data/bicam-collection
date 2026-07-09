{{ config(tags=["bills", "staging"]) }}

with cosponsor_responses as (
    {{ resolve_child('bill_detail', 'bill_cosponsors', 'bills') }}
),
bill_detail as (
    select
        raw_id,
        bill_id
    from {{ ref('stg_congress__bill_detail') }}
),
unnested as (
    select
        bill_detail.bill_id,
        cosponsor_item.value as cosponsor_json
    from cosponsor_responses
    join bill_detail
      on cosponsor_responses.parent_raw_id = bill_detail.raw_id
    cross join lateral {{ unnest_array('response', 'cosponsors', 'cosponsor_item') }}
)

select
    bill_id,
    nullif(cosponsor_json #>> '{bioguideId}', '') as bioguide_id,
    nullif(cosponsor_json #>> '{fullName}', '') as display_name,
    nullif(cosponsor_json #>> '{party}', '') as party,
    nullif(cosponsor_json #>> '{state}', '') as state,
    (nullif(cosponsor_json #>> '{district}', ''))::integer as district,
    coalesce((nullif(cosponsor_json #>> '{isOriginalCosponsor}', ''))::boolean, false) as is_original_cosponsor,
    (nullif(cosponsor_json #>> '{sponsorshipDate}', ''))::date as sponsorship_date,
    (nullif(cosponsor_json #>> '{sponsorshipWithdrawnDate}', ''))::date as sponsorship_withdrawal_date
from unnested
