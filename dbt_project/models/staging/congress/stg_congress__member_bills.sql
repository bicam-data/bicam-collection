{{ config(tags=["members", "staging"]) }}

with bill_rows as (
    select
        source_raw_id as raw_id,
        member_detail_raw_id as parent_raw_id,
        source_fetched_at as child_fetched_at,
        source_ordinal as array_ordinal,
        sponsorship_type,
        lower(legislation_type) as bill_type,
        {{ normalize_congress_bill_number_text('legislation_number') }} as bill_number,
        congress
    from {{ ref('int_congress_member_legislation_observations') }}
    where legislation_type is not null
      and legislation_number is not null
      and amendment_number is null
), extracted as (
select
    members.bioguide_id,
    bill_rows.raw_id,
    bill_rows.child_fetched_at,
    bill_rows.array_ordinal,
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
), keyed as (
    select
        extracted.*,
        row_number() over (
            partition by bioguide_id, bill_id, sponsorship_type
            order by child_fetched_at desc nulls last,
                     raw_id desc,
                     array_ordinal desc
        ) as key_rank,
        bioguide_id is not null and bill_id is not null as key_complete
    from extracted
)

select bioguide_id, bill_id, sponsorship_type, bill_type, bill_number, congress
from keyed
where key_rank = 1 or not key_complete
