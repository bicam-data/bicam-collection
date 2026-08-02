{{ config(tags=["bill_reconciliation", "intermediate"]) }}

select
    'congress:' || bill_id as source_record_id,
    'congress' as source_name,
    raw_id as source_raw_id,
    raw_response_version_id as source_raw_response_version_id,
    endpoint_url as source_endpoint_url,
    {{ bill_canonical_id('bill_type', 'bill_number', 'congress') }} as canonical_bill_id,
    bill_type,
    bill_number,
    congress,
    title,
    {{ normalize_match_text('title') }} as normalized_title,
    {{ normalize_chamber('origin_chamber') }} as origin_chamber,
    introduced_at,
    updated_at as source_updated_at
from {{ ref('stg_congress__bill_detail') }}
