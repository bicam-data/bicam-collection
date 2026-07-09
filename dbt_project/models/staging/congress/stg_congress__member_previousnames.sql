{{ config(tags=["members", "staging"]) }}

with member_raw as (
    select
        raw.id as raw_id,
        raw.response -> 'member' as member_json
    from {{ source('raw', 'raw_responses') }} as raw
    where raw.node_type = 'member_detail'
),
previous_names as (
    select
        member_raw.raw_id,
        previous_name.value as previous_name_json
    from member_raw
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(member_raw.member_json -> 'previousNames') = 'array' then member_raw.member_json -> 'previousNames'
            else '[]'::jsonb
        end
    ) as previous_name(value)
),
normalized as (
    select
        raw_id,
        {{ json_text('previous_name_json', 'directOrderName') }} as direct_order_name,
        {{ json_text('previous_name_json', 'invertedOrderName') }} as inverted_order_name,
        {{ json_text('previous_name_json', 'firstName') }} as first_name,
        {{ json_text('previous_name_json', 'middleName') }} as middle_name,
        {{ json_text('previous_name_json', 'lastName') }} as last_name,
        {{ json_text('previous_name_json', 'honorificName') }} as honorific_prefix,
        {{ json_timestamptz('previous_name_json', 'startDate') }} as start_date,
        {{ json_timestamptz('previous_name_json', 'endDate') }} as end_date
    from previous_names
),
current_name_signature as (
    select distinct on (raw_id)
        raw_id,
        direct_order_name,
        inverted_order_name,
        first_name,
        middle_name,
        last_name,
        honorific_prefix
    from normalized
    order by raw_id, (end_date is null) desc, start_date desc nulls last
),
deduped as (
    select normalized.*
    from normalized
    left join current_name_signature
        on normalized.raw_id = current_name_signature.raw_id
    where normalized.end_date is null
       or not (
            normalized.direct_order_name is not distinct from current_name_signature.direct_order_name
        and normalized.inverted_order_name is not distinct from current_name_signature.inverted_order_name
        and normalized.first_name is not distinct from current_name_signature.first_name
        and normalized.middle_name is not distinct from current_name_signature.middle_name
        and normalized.last_name is not distinct from current_name_signature.last_name
        and normalized.honorific_prefix is not distinct from current_name_signature.honorific_prefix
       )
)

select
    members.bioguide_id,
    deduped.direct_order_name,
    deduped.inverted_order_name,
    deduped.first_name,
    deduped.middle_name,
    deduped.last_name,
    deduped.honorific_prefix,
    deduped.start_date,
    deduped.end_date
from deduped
inner join {{ ref('stg_congress__member_detail') }} as members
    on deduped.raw_id = members.raw_id
