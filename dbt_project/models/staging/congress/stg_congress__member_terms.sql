{{ config(tags=["members", "staging"]) }}

with member_raw as (
    select
        raw.raw_id as raw_id,
        raw.response -> 'member' as member_json
    from {{ ref('int_raw_responses__current') }} as raw
    where raw.node_type = 'member_detail'
),
terms as (
    select
        member_raw.raw_id,
        term.value as term_json
    from member_raw
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(member_raw.member_json -> 'terms') = 'array' then member_raw.member_json -> 'terms'
            else '[]'::jsonb
        end
    ) as term(value)
)

select
    members.bioguide_id,
    {{ json_text('term_json', 'memberType') }} as member_type,
    lower({{ json_text('term_json', 'chamber') }}) as chamber,
    {{ json_int('term_json', 'congress') }} as congress,
    {{ json_int('term_json', 'startYear') }} as start_year,
    {{ json_int('term_json', 'endYear') }} as end_year,
    {{ json_text('term_json', 'stateName') }} as state_name,
    {{ json_text('term_json', 'stateCode') }} as state_code,
    {{ json_int('term_json', 'district') }} as district
from terms
inner join {{ ref('stg_congress__member_detail') }} as members
    on terms.raw_id = members.raw_id
