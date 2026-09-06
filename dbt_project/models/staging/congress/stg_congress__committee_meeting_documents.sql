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
        meeting_document.value as document_json
    from detail
    cross join lateral {{ unnest_array('meeting_json', 'meetingDocuments', 'meeting_document') }}
)

select
    meeting_id,
    {{ json_text('document_json', 'name') }} as document_name,
    {{ json_text('document_json', 'documentType') }} as document_type,
    {{ json_text('document_json', 'description') }} as description,
    {{ json_text('document_json', 'format') }} as format,
    {{ json_text('document_json', 'url') }} as meeting_doc_url
from unnested
