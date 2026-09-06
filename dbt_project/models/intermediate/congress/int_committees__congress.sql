{{ config(tags=["committees", "congress", "intermediate"]) }}

select
    'congress:' || committee_id as source_record_id,
    committee_id as canonical_committee_id,
    raw_id as source_raw_id,
    endpoint_url as source_endpoint_url,
    raw_response_version_id as source_raw_response_version_id,
    system_code,
    system_number,
    name,
    chamber,
    committee_type,
    is_current,
    committee_url,
    bills_count,
    reports_count,
    nominations_count,
    updated_at
from {{ ref('stg_congress__committee_detail') }}
