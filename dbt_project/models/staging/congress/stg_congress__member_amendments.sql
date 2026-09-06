{{ config(tags=["members", "staging"]) }}

with source_rows as (
    select
        bioguide_id,
        member_detail_raw_id as parent_raw_id,
        amendment_number,
        congress,
        legislation_url,
        sponsorship_type,
        source_fetched_at,
        source_raw_id,
        source_ordinal
    from {{ ref('int_congress_member_legislation_observations') }}
    where amendment_number is not null
), sponsored_legislation as (
    select
        bioguide_id,
        parent_raw_id,
        amendment_number,
        congress,
        legislation_url,
        sponsorship_type,
        source_fetched_at,
        source_raw_id,
        source_ordinal
    from source_rows
), keyed as (
    select
        sponsored_legislation.*,
        {{ build_custom_id([
            "lower(" ~ amendment_type_from_url('sponsored_legislation.legislation_url', 'sponsored_legislation.congress', 'sponsored_legislation.amendment_number') ~ ")",
            "sponsored_legislation.amendment_number"
        ]) }} || '-' || sponsored_legislation.congress::text as amendment_id,
        row_number() over (
            partition by bioguide_id,
                         {{ build_custom_id([
                             "lower(" ~ amendment_type_from_url('sponsored_legislation.legislation_url', 'sponsored_legislation.congress', 'sponsored_legislation.amendment_number') ~ ")",
                             "sponsored_legislation.amendment_number"
                         ]) }} || '-' || congress::text,
                         sponsorship_type
            order by source_fetched_at desc nulls last,
                     source_raw_id desc,
                     source_ordinal desc
        ) as key_rank
    from sponsored_legislation
)

select
    bioguide_id,
    amendment_id,
    sponsorship_type,
    {{ amendment_type_from_url('legislation_url', 'congress', 'amendment_number') }} as amendment_type,
    amendment_number,
    congress
from keyed
where key_rank = 1
