{{ config(tags=["members", "staging"]) }}

with sponsored_legislation as (
    select
        parent_raw_id,
        amendment_number,
        congress,
        legislation_url,
        'sponsor' as sponsorship_type
    from {{ ref('stg_congress__member_sponsored_legislation') }}
    where amendment_number is not null
),
cosponsored_legislation as (
    select
        parent_raw_id,
        amendment_number,
        congress,
        legislation_url,
        'cosponsor' as sponsorship_type
    from {{ ref('stg_congress__member_cosponsored_legislation') }}
    where amendment_number is not null
),
amendment_rows as (
    select * from sponsored_legislation
    union all
    select * from cosponsored_legislation
)

select
    members.bioguide_id,
    {{ build_custom_id([
        "(" ~ amendment_type_from_url('amendment_rows.legislation_url', 'amendment_rows.congress', 'amendment_rows.amendment_number') ~ ")",
        "amendment_rows.amendment_number"
    ]) }} || '-' || amendment_rows.congress::text as amendment_id,
    amendment_rows.sponsorship_type,
    {{ amendment_type_from_url('amendment_rows.legislation_url', 'amendment_rows.congress', 'amendment_rows.amendment_number') }} as amendment_type,
    amendment_rows.amendment_number,
    amendment_rows.congress
from amendment_rows
inner join {{ ref('stg_congress__member_detail') }} as members
    on amendment_rows.parent_raw_id = members.raw_id
