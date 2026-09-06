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
        hearing.value as hearing_json
    from detail
    cross join lateral {{ unnest_array('meeting_json', 'hearingTranscript', 'hearing') }}
),
parsed as (
    select
        meeting_id,
        hearing_json,
        substring({{ json_text('hearing_json', 'url') }} from '/hearing/([0-9]+)/') as congress_text,
        {{ congress_hearing_route_chamber(
            "substring(" ~ json_text('hearing_json', 'url') ~ " from '/hearing/[0-9]+/([^/]+)/')"
        ) }} as chamber
    from unnested
)

select
    meeting_id,
    lower(left(chamber, 1))
    || 'hrg'
    || {{ json_bigint('hearing_json', 'jacketNumber') }}::text
    || '-'
    || congress_text as hearing_id,
    lower(left(chamber, 1)) || 'hrg' as hearing_type,
    {{ json_bigint('hearing_json', 'jacketNumber') }} as hearing_jacketnumber,
    chamber,
    congress_text::integer as congress
from parsed
