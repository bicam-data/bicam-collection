{{ config(tags=["members", "staging"]) }}

with sponsored_responses as (
    {{ resolve_child('member_detail', 'member_sponsored_legislation', 'members') }}
),
sponsored_legislation as (
    select
        parent_raw_id,
        legislation.value as legislation_json,
        'sponsor' as sponsorship_type
    from sponsored_responses
    cross join lateral {{ unnest_array('response', 'sponsoredLegislation', 'legislation') }}
),
cosponsored_responses as (
    {{ resolve_child('member_detail', 'member_cosponsored_legislation', 'members') }}
),
cosponsored_legislation as (
    select
        parent_raw_id,
        legislation.value as legislation_json,
        'cosponsor' as sponsorship_type
    from cosponsored_responses
    cross join lateral {{ unnest_array('response', 'cosponsoredLegislation', 'legislation') }}
),
bill_legislation as (
    select * from sponsored_legislation
    union all
    select * from cosponsored_legislation
),
bill_rows as (
    select
        parent_raw_id,
        sponsorship_type,
        {{ json_text('legislation_json', 'type') }} as bill_type,
        {{ json_text('legislation_json', 'number') }} as bill_number,
        {{ json_int('legislation_json', 'congress') }} as congress
    from bill_legislation
    where {{ json_text('legislation_json', 'type') }} is not null
      and {{ json_text('legislation_json', 'number') }} is not null
)

select
    members.bioguide_id,
    {{ build_custom_id([
        "lower(bill_rows.bill_type)",
        "bill_rows.bill_number"
    ]) }} || '-' || bill_rows.congress::text as bill_id,
    bill_rows.sponsorship_type,
    lower(bill_rows.bill_type) as bill_type,
    bill_rows.bill_number,
    bill_rows.congress
from bill_rows
inner join {{ ref('stg_congress__member_detail') }} as members
    on bill_rows.parent_raw_id = members.raw_id
