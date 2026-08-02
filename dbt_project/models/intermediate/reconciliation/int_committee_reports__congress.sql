{{ config(tags=["committee_report_reconciliation", "intermediate"]) }}

select
    'congress:' || report_part_id as source_record_id,
    'congress' as source_name,
    raw_id as source_raw_id,
    raw_response_version_id as source_raw_response_version_id,
    endpoint_url as source_endpoint_url,
    {{ committee_report_canonical_id(
        'report_type', 'report_number', 'congress', 'report_part', 'is_errata'
    ) }} as canonical_report_id,
    report_type,
    report_number,
    congress,
    report_part,
    is_errata,
    title,
    {{ normalize_match_text('title') }} as normalized_title,
    {{ normalize_chamber('chamber') }} as chamber,
    issued_at,
    updated_at as source_updated_at
from {{ ref('stg_congress__committee_report_detail') }}
