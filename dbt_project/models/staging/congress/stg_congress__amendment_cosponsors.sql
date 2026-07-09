{{ config(tags=["amendments", "staging"]) }}

with cosponsor_responses as (
    {{ resolve_child('amendment_detail', 'amendment_cosponsors', 'amendments') }}
),
amendment_detail as (
    select
        raw_id,
        amendment_id
    from {{ ref('stg_congress__amendment_detail') }}
),
unnested as (
    select
        amendment_detail.amendment_id,
        cosponsor.value as cosponsor_json
    from cosponsor_responses
    join amendment_detail
      on cosponsor_responses.parent_raw_id = amendment_detail.raw_id
    cross join lateral {{ unnest_array('response', 'cosponsors', 'cosponsor') }}
)

select
    amendment_id,
    {{ json_text('cosponsor_json', 'bioguideId') }} as bioguide_id,
    {{ json_text('cosponsor_json', 'fullName') }} as display_name,
    {{ json_text('cosponsor_json', 'firstName') }} as first_name,
    {{ json_text('cosponsor_json', 'middleName') }} as middle_name,
    {{ json_text('cosponsor_json', 'lastName') }} as last_name,
    {{ json_text('cosponsor_json', 'party') }} as party,
    {{ json_text('cosponsor_json', 'state') }} as state,
    {{ json_int('cosponsor_json', 'district') }} as district,
    {{ json_boolean('cosponsor_json', 'isOriginalCosponsor') }} as is_original_cosponsor,
    {{ json_date('cosponsor_json', 'sponsorshipDate') }} as sponsorship_date,
    {{ json_date('cosponsor_json', 'sponsorshipWithdrawnDate') }} as sponsorship_withdrawal_date
from unnested
