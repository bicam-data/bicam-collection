{{ config(tags=["senate_communications", "staging"]) }}

with detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        case
            when jsonb_typeof(response -> 'senateCommunication') = 'object' then response -> 'senateCommunication'
            else response
        end as communication_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'senate-communications'
      and node_type = 'senate_communication_detail'
), typed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        'senate-' || lower({{ json_text('communication_json', 'communicationType.code') }})
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
        coalesce(jsonb_array_length(communication_json -> 'committees'), 0) as committees_count,
        {{ json_timestamptz('communication_json', 'updateDate') }} as updated_at
    from detail
)
select *
from typed
