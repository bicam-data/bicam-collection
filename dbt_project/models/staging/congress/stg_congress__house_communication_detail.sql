{{ config(tags=["house_communications", "staging"]) }}

with detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        case
            when jsonb_typeof(response -> 'houseCommunication') = 'object' then response -> 'houseCommunication'
            else response
        end as communication_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'house-communications'
      and node_type = 'house_communication_detail'
), typed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        'house-' || lower({{ json_text('communication_json', 'communicationType.code') }})
        || '-' || {{ json_text('communication_json', 'number') }}
        || '-' || {{ json_int('communication_json', 'congress') }}::text as communication_id,
        lower({{ json_text('communication_json', 'chamber') }}) as chamber,
        lower({{ json_text('communication_json', 'communicationType.code') }}) as communication_type_code,
        {{ json_text('communication_json', 'communicationType.name') }} as communication_type_name,
        {{ json_int('communication_json', 'congress') }} as congress,
        {{ json_text('communication_json', 'number') }} as communication_number,
        {{ json_text('communication_json', 'abstract') }} as abstract,
        {{ json_date('communication_json', 'congressionalRecordDate') }} as congressional_record_date,
        {{ json_int('communication_json', 'sessionNumber') }} as session_number,
        {{ json_boolean('communication_json', 'isRulemaking') }} as is_rulemaking,
        {{ json_text('communication_json', 'reportNature') }} as report_nature,
        {{ json_text('communication_json', 'submittingAgency') }} as submitting_agency,
        {{ json_text('communication_json', 'submittingOfficial') }} as submitting_official,
        {{ json_text('communication_json', 'legalAuthority') }} as legal_authority,
        coalesce(jsonb_array_length(communication_json -> 'committees'), 0) as committees_count,
        coalesce(jsonb_array_length(communication_json -> 'matchingRequirements'), 0) as matching_requirements_count,
        {{ json_timestamptz('communication_json', 'updateDate') }} as updated_at
    from detail
)
select *
from typed
