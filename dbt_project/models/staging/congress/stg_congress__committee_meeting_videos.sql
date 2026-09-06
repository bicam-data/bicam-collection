{{ config(tags=["committee_meetings", "staging"]) }}

with detail as (
    select
        response -> 'committeeMeeting' as meeting_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'committee-meetings'
      and node_type = 'committee_meeting_detail'
),
unnested as (
    select
        {{ json_text('meeting_json', 'eventId') }}
        || '-'
        || {{ json_int('meeting_json', 'congress') }}::text as meeting_id,
        video.value as video_json
    from detail
    cross join lateral {{ unnest_array('meeting_json', 'videos', 'video') }}
)

select
    meeting_id,
    {{ json_text('video_json', 'name') }} as video_name,
    {{ json_text('video_json', 'url') }} as video_url
from unnested
