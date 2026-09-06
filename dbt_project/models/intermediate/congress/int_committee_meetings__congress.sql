{{ config(tags=["committee_meetings", "congress", "intermediate"]) }}

select
    'congress:' || meeting_id as source_record_id,
    meeting_id as canonical_committee_meeting_id,
    raw_id as source_raw_id,
    endpoint_url as source_endpoint_url,
    raw_response_version_id as source_raw_response_version_id,
    meeting_id,
    title,
    meeting_type,
    chamber,
    congress,
    date,
    room,
    street_address,
    building,
    city,
    state,
    zip_code,
    meeting_status,
    updated_at
from {{ ref('stg_congress__committee_meeting_detail') }}
