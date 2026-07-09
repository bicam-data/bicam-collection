{{ config(tags=["members", "staging"]) }}

with detail as (
    select
        id as raw_id,
        endpoint_url,
        response -> 'member' as member_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'members'
      and node_type = 'member_detail'
),
party_history as (
    select
        detail.raw_id,
        party_item.value as party_json,
        row_number() over (
            partition by detail.raw_id
            order by coalesce((nullif(party_item.value #>> '{startYear}', ''))::integer, 0) desc
        ) as party_rank
    from detail
    cross join lateral jsonb_array_elements(
        coalesce(detail.member_json #> '{partyHistory}', '[]'::jsonb)
    ) as party_item(value)
),
latest_party as (
    select
        raw_id,
        nullif(party_json #>> '{partyName}', '') as current_party
    from party_history
    where party_rank = 1
)

select
    detail.raw_id,
    detail.endpoint_url,
    {{ json_text('member_json', 'bioguideId') }} as bioguide_id,
    upper(concat_ws(
        ' ',
        {{ json_text('member_json', 'firstName') }},
        {{ json_text('member_json', 'middleName') }},
        {{ json_text('member_json', 'lastName') }},
        {{ json_text('member_json', 'suffixName') }}
    )) as normalized_name,
    {{ json_text('member_json', 'directOrderName') }} as direct_order_name,
    {{ json_text('member_json', 'invertedOrderName') }} as inverted_order_name,
    {{ json_text('member_json', 'honorificName') }} as honorific_prefix,
    {{ json_text('member_json', 'firstName') }} as first_name,
    {{ json_text('member_json', 'middleName') }} as middle_name,
    {{ json_text('member_json', 'lastName') }} as last_name,
    {{ json_text('member_json', 'suffixName') }} as suffix,
    {{ json_text('member_json', 'nickName') }} as nickname,
    latest_party.current_party,
    {{ json_text('member_json', 'state') }} as state,
    {{ json_int('member_json', 'district') }} as district,
    {{ json_int('member_json', 'birthYear') }} as birth_year,
    {{ json_int('member_json', 'deathYear') }} as death_year,
    coalesce(
        {{ json_text('member_json', 'officialUrl') }},
        {{ json_text('member_json', 'officialWebsiteUrl') }}
    ) as official_url,
    {{ json_text('member_json', 'addressInformation.officeAddress') }} as office_address,
    {{ json_text('member_json', 'addressInformation.city') }} as office_city,
    {{ json_text('member_json', 'addressInformation.district') }} as office_district,
    {{ json_text('member_json', 'addressInformation.zipCode') }} as office_zip,
    {{ json_text('member_json', 'addressInformation.phoneNumber') }} as office_phone,
    {{ json_text('member_json', 'depiction.imageUrl') }} as image_url,
    {{ json_text('member_json', 'depiction.attribution') }} as image_attribution,
    {{ json_boolean('member_json', 'currentMember') }} as is_current_member,
    {{ json_int('member_json', 'sponsoredLegislation.count', 0) }} as sponsored_legislation_count,
    {{ json_int('member_json', 'cosponsoredLegislation.count', 0) }} as cosponsored_legislation_count,
    {{ json_timestamptz('member_json', 'updateDate') }} as updated_at
from detail
left join latest_party
  on detail.raw_id = latest_party.raw_id
