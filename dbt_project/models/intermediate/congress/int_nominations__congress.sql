{{ config(tags=["nominations", "congress", "intermediate"]) }}

select
    'congress:' || nomination_part_id as source_record_id,
    nomination_part_id as canonical_nomination_id,
    raw_id as source_raw_id,
    endpoint_url as source_endpoint_url,
    raw_response_version_id as source_raw_response_version_id,
    nomination_id,
    nomination_number,
    nomination_part,
    congress,
    description,
    is_privileged,
    is_civilian,
    is_special,
    received_at,
    authority_date,
    executive_calendar_number,
    citation,
    actions_count,
    committees_count,
    updated_at
from {{ ref('stg_congress__nomination_detail') }}
