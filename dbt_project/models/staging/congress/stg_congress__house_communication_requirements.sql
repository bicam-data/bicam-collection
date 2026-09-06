{{ config(tags=["house_communications", "staging"]) }}

with detail as (
    select
        raw_id,
        case
            when jsonb_typeof(response -> 'houseCommunication') = 'object' then response -> 'houseCommunication'
            else response
        end as communication_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'house-communications'
      and node_type = 'house_communication_detail'
), unnested as (
    select
        raw_id,
        communication_json,
        requirement.value as requirement_json
    from detail
    cross join lateral {{ unnest_array('communication_json', 'matchingRequirements', 'requirement') }}
)
select
    raw_id,
    'house-' || lower({{ json_text('communication_json', 'communicationType.code') }})
    || '-' || {{ json_text('communication_json', 'number') }}
    || '-' || {{ json_int('communication_json', 'congress') }}::text as communication_id,
    {{ json_int('requirement_json', 'number') }} as requirement_number,
    {{ json_text('requirement_json', 'url') }} as requirement_url
from unnested
