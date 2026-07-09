{{ config(tags=["committee_meetings", "staging"]) }}

with detail as (
    select
        id as parent_raw_id,
        endpoint_url as parent_endpoint_url,
        response -> 'committeeMeeting' as meeting_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'committee-meetings'
      and node_type = 'committee_meeting_detail'
      and response #> '{committeeMeeting,location}' is not null
),
location_base as (
    select
        parent_raw_id,
        parent_endpoint_url,
        meeting_json #> '{location}' as location_json,
        case
            when coalesce(meeting_json #>> '{location,address}', '') = '' then null::jsonb
            else (meeting_json #>> '{location,address}')::jsonb
        end as address_json
    from detail
)

select
    parent_raw_id,
    parent_endpoint_url,
    nullif(location_json #>> '{building}', '') as building,
    nullif(location_json #>> '{room}', '') as room,
    nullif(address_json #>> '{building_name}', '') as address_building_name,
    nullif(address_json #>> '{street-address}', '') as address_street_address,
    nullif(address_json #>> '{city}', '') as address_city,
    nullif(address_json #>> '{state}', '') as address_state,
    nullif(address_json #>> '{postal_code}', '') as address_postal_code
from location_base
