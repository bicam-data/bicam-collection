{{ config(tags=["committee_meetings", "staging"]) }}

with detail as (
    select
        response -> 'committeeMeeting' as meeting_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'committee-meetings'
      and node_type = 'committee_meeting_detail'
),
unnested as (
    select
        {{ json_text('meeting_json', 'eventId') }}
        || '-'
        || {{ json_int('meeting_json', 'congress') }}::text as meeting_id,
        witness.value as witness_json
    from detail
    cross join lateral {{ unnest_array('meeting_json', 'witnesses', 'witness') }}
)

select
    meeting_id,
    {{ json_text('witness_json', 'name') }} as witness_name,
    {{ json_text('witness_json', 'position') }} as witness_position,
    {{ json_text('witness_json', 'organization') }} as witness_org
from unnested
