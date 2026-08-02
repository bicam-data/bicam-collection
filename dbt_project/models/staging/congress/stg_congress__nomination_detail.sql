{{ config(tags=["nominations", "staging"]) }}

with detail as (
    select
        id as raw_id,
        endpoint_url,
        raw_response_version_id,
        response -> 'nomination' as nomination_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'nominations'
      and node_type = 'nomination_detail'
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    'pn'
    || {{ json_text('nomination_json', 'number') }}
    || '-'
    || {{ json_int('nomination_json', 'congress') }}::text as nomination_id,
    'pn'
    || {{ json_text('nomination_json', 'number') }}
    || '-'
    || coalesce({{ json_text('nomination_json', 'partNumber') }}, '00')
    || '-'
    || {{ json_int('nomination_json', 'congress') }}::text as nomination_part_id,
    {{ json_text('nomination_json', 'number') }} as nomination_number,
    coalesce({{ json_text('nomination_json', 'partNumber') }}, '00') as nomination_part,
    {{ json_int('nomination_json', 'congress') }} as congress,
    {{ json_text('nomination_json', 'description') }} as description,
    {{ json_boolean('nomination_json', 'isPrivileged') }} as is_privileged,
    {{ json_boolean('nomination_json', 'nominationType.isCivilian') }} as is_civilian,
    {{ json_boolean('nomination_json', 'isList') }} as is_special,
    {{ json_date('nomination_json', 'receivedDate') }} as received_at,
    {{ json_date('nomination_json', 'authorityDate') }} as authority_date,
    {{ json_text('nomination_json', 'executiveCalendarNumber') }} as executive_calendar_number,
    {{ json_text('nomination_json', 'citation') }} as citation,
    coalesce({{ json_int('nomination_json', 'actions.count') }}, 0) as actions_count,
    coalesce({{ json_int('nomination_json', 'committees.count') }}, 0) as committees_count,
    {{ json_timestamptz('nomination_json', 'updateDate') }} as updated_at
from detail
