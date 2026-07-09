{{ config(tags=["members", "staging"]) }}

with member_raw as (
    select
        raw.id as raw_id,
        raw.response -> 'member' as member_json
    from {{ source('raw', 'raw_responses') }} as raw
    where raw.node_type = 'member_detail'
),
party_history as (
    select
        member_raw.raw_id,
        party.value as party_json
    from member_raw
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(member_raw.member_json -> 'partyHistory') = 'array' then member_raw.member_json -> 'partyHistory'
            else '[]'::jsonb
        end
    ) as party(value)
)

select
    members.bioguide_id,
    {{ json_text('party_json', 'partyAbbreviation') }} as party_code,
    {{ json_text('party_json', 'partyName') }} as party_name,
    {{ json_int('party_json', 'startYear') }} as start_year,
    {{ json_int('party_json', 'endYear') }} as end_year
from party_history
inner join {{ ref('stg_congress__member_detail') }} as members
    on party_history.raw_id = members.raw_id
