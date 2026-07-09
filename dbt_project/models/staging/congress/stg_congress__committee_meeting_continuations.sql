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
        continuation.value as continuation_json
    from detail
    cross join lateral {{ unnest_array('meeting_json', 'continuations', 'continuation') }}
)

select
    meeting_id,
    {{ json_timestamptz('continuation_json', 'continuationDate') }} as continuation_date
from unnested
