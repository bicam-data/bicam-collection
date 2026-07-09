{{ config(tags=["members", "staging"]) }}

with member_raw as (
    select
        raw.id as raw_id,
        raw.response -> 'member' as member_json
    from {{ source('raw', 'raw_responses') }} as raw
    where raw.node_type = 'member_detail'
),
member_terms as (
    select
        member_raw.raw_id,
        (term.value ->> 'congress')::integer as congress,
        lower(nullif(term.value ->> 'chamber', '')) as chamber
    from member_raw
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(member_raw.member_json -> 'terms') = 'array' then member_raw.member_json -> 'terms'
            else '[]'::jsonb
        end
    ) as term(value)
),
leadership_roles as (
    select
        member_raw.raw_id,
        leadership.value as leadership_json
    from member_raw
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(member_raw.member_json -> 'leadership') = 'array' then member_raw.member_json -> 'leadership'
            else '[]'::jsonb
        end
    ) as leadership(value)
)

select
    members.bioguide_id,
    {{ json_text('leadership_json', 'type') }} as role,
    {{ json_int('leadership_json', 'congress') }} as congress,
    member_terms.chamber,
    {{ json_boolean('leadership_json', 'current', false) }} as is_current
from leadership_roles
inner join {{ ref('stg_congress__member_detail') }} as members
    on leadership_roles.raw_id = members.raw_id
left join member_terms
    on leadership_roles.raw_id = member_terms.raw_id
   and {{ json_int('leadership_json', 'congress') }} = member_terms.congress
