{{ config(tags=["committee_report_reconciliation", "intermediate"]) }}

with ranked as (
    select
        'govinfo:' || report_id as source_record_id,
        'govinfo' as source_name,
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
        last_modified as source_updated_at,
        row_number() over (
            partition by report_id
            order by last_modified desc nulls last, package_id desc
        ) as _rn
    from {{ ref('stg_govinfo__committeereports') }}
)

select
    source_record_id,
    source_name,
    source_raw_id,
    source_raw_response_version_id,
    source_endpoint_url,
    canonical_report_id,
    report_type,
    report_number,
    congress,
    report_part,
    is_errata,
    title,
    normalized_title,
    chamber,
    issued_at,
    source_updated_at
from ranked
where _rn = 1
