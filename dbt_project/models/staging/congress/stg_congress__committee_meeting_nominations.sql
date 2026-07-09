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
        nomination.value as nomination_json
    from detail
    cross join lateral {{ unnest_array('meeting_json', 'relatedItems.nominations', 'nomination') }}
)

select
    meeting_id,
    'pn'
    || {{ json_text('nomination_json', 'number') }}
    || '-'
    || coalesce({{ json_text('nomination_json', 'part') }}, '00')
    || '-'
    || {{ json_int('nomination_json', 'congress') }}::text as nomination_id,
    {{ json_text('nomination_json', 'number') }} as nomination_number,
    {{ json_text('nomination_json', 'part') }} as nomination_part,
    {{ json_int('nomination_json', 'congress') }} as congress
from unnested
