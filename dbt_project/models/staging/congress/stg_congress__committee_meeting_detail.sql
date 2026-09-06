{{ config(tags=["committee_meetings", "staging"]) }}

with detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response -> 'committeeMeeting' as meeting_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'committee-meetings'
      and node_type = 'committee_meeting_detail'
),
base as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        meeting_json,
        meeting_json -> 'location' as location_json,
        case
            when jsonb_typeof(meeting_json -> 'location' -> 'address') = 'object'
                then meeting_json -> 'location' -> 'address'
            when jsonb_typeof(meeting_json -> 'location' -> 'address') = 'string'
                 and left(coalesce(meeting_json #>> '{location,address}', ''), 1) = '{'
                then (meeting_json #>> '{location,address}')::jsonb
            else null
        end as address_json
    from detail
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ json_text('meeting_json', 'eventId') }}
    || '-'
    || {{ json_int('meeting_json', 'congress') }}::text as meeting_id,
    {{ json_text('meeting_json', 'title') }} as title,
    {{ json_text('meeting_json', 'type') }} as meeting_type,
    lower({{ json_text('meeting_json', 'chamber') }}) as chamber,
    {{ json_int('meeting_json', 'congress') }} as congress,
    {{ json_timestamptz('meeting_json', 'date') }} as date,
    nullif(location_json ->> 'room', '') as room,
    nullif(address_json ->> 'street-address', '') as street_address,
    coalesce(
        nullif(location_json ->> 'building', ''),
        nullif(address_json ->> 'building_name', '')
    ) as building,
    nullif(address_json ->> 'city', '') as city,
    nullif(address_json ->> 'state', '') as state,
    nullif(address_json ->> 'postal_code', '') as zip_code,
    {{ json_text('meeting_json', 'meetingStatus') }} as meeting_status,
    {{ json_timestamptz('meeting_json', 'updateDate') }} as updated_at
from base
