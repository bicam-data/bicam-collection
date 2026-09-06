{{ config(tags=["house_requirements", "staging"]) }}

with communication_responses as (
    {{ resolve_child('house_requirement_detail', 'house_requirement_matching_communications', 'house-requirements') }}
), unnested as (
    select
        parent_raw_id,
        parent_endpoint_url,
        communication.value as communication_json
    from communication_responses
    cross join lateral {{ unnest_array('response', 'matchingCommunications', 'communication') }}
)
select
    parent_raw_id as requirement_raw_id,
    substring(parent_endpoint_url from '/house-requirement/([0-9]+)')::integer as requirement_number,
    lower({{ json_text('communication_json', 'chamber') }}) as chamber,
    lower({{ json_text('communication_json', 'communicationType.code') }}) as communication_type_code,
    {{ json_int('communication_json', 'congress') }} as congress,
    {{ json_int('communication_json', 'number') }} as communication_number,
    {{ json_text('communication_json', 'url') }} as communication_url
from unnested
