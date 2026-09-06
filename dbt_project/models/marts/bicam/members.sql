{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'members'],
    indexes=[
      {'columns': ['bioguide_id'], 'unique': true},
      {'columns': ['normalized_name']},
      {'columns': ['state', 'is_current_member']}
    ]
) }}

select
    bioguide_id,
    null::text as icpsr,
    normalized_name,
    direct_order_name,
    inverted_order_name,
    honorific_prefix,
    first_name,
    middle_name,
    last_name,
    suffix,
    nickname,
    current_party as party,
    state,
    district::text as district,
    birth_year,
    death_year,
    official_url,
    office_address,
    office_city,
    office_district,
    office_zip,
    office_phone,
    sponsored_legislation_count,
    cosponsored_legislation_count,
    image_url as depiction_image_url,
    image_attribution as depiction_attribution,
    is_current_member,
    updated_at
from {{ ref('stg_congress__member_detail') }}
where bioguide_id is not null
