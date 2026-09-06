{{ config(tags=["amendments", "staging"]) }}

with detail as (
    select
        amendment_detail.amendment_id,
        raw_responses.response -> 'amendment' as amendment_json
    from {{ ref('int_raw_responses__current') }} as raw_responses
    join {{ ref('stg_congress__amendment_detail') }} as amendment_detail
      on raw_responses.raw_id = amendment_detail.raw_id
    where raw_responses.source = 'congress'
      and raw_responses.category = 'amendments'
      and raw_responses.node_type = 'amendment_detail'
),
unnested as (
    select
        amendment_id,
        sponsor.value as sponsor_json
    from detail
    cross join lateral {{ unnest_array('amendment_json', 'sponsors', 'sponsor') }}
)

select
    amendment_id,
    {{ json_text('sponsor_json', 'bioguideId') }} as bioguide_id,
    {{ json_text('sponsor_json', 'fullName') }} as full_name,
    {{ json_text('sponsor_json', 'firstName') }} as first_name,
    {{ json_text('sponsor_json', 'middleName') }} as middle_name,
    {{ json_text('sponsor_json', 'lastName') }} as last_name,
    {{ json_text('sponsor_json', 'party') }} as party,
    {{ json_text('sponsor_json', 'state') }} as state,
    {{ json_int('sponsor_json', 'district') }} as district
from unnested
where {{ json_text('sponsor_json', 'bioguideId') }} is not null
