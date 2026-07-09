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
        treaty.value as treaty_json
    from detail
    cross join lateral {{ unnest_array('meeting_json', 'relatedItems.treaties', 'treaty') }}
)

select
    meeting_id,
    'td'
    || {{ json_int('treaty_json', 'congress') }}::text
    || '-'
    || {{ json_text('treaty_json', 'number') }} as treaty_id,
    'td'
    || {{ json_int('treaty_json', 'congress') }}::text
    || '-'
    || {{ json_text('treaty_json', 'number') }}
    || coalesce({{ json_text('treaty_json', 'part') }}, '') as treaty_part_id,
    {{ json_int('treaty_json', 'congress') }} as congress,
    {{ json_text('treaty_json', 'number') }} as treaty_number,
    {{ json_text('treaty_json', 'part') }} as treaty_part
from unnested
