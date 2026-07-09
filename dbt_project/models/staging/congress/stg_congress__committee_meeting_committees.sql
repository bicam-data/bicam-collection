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
        committee.value as committee_json
    from detail
    cross join lateral {{ unnest_array('meeting_json', 'committees', 'committee') }}
)

select
    meeting_id,
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    {{ json_text('committee_json', 'name') }} as committee_name
from unnested
