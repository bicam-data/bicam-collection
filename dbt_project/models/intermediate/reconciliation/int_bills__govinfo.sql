{{ config(tags=["bill_reconciliation", "intermediate"]) }}

with ranked as (
    select
        'govinfo:' || package_id as source_record_id,
        'govinfo' as source_name,
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
        issued_at,
        last_modified as source_updated_at,
        row_number() over (
            partition by bill_type, bill_number, congress
            order by last_modified desc nulls last, package_id desc
        ) as _rn
    from {{ ref('stg_govinfo__bills_packages') }}
)

select
    source_record_id,
    source_name,
    source_raw_id,
    source_raw_response_version_id,
    source_endpoint_url,
    canonical_bill_id,
    bill_type,
    bill_number,
    congress,
    title,
    normalized_title,
    origin_chamber,
    issued_at,
    source_updated_at
from ranked
where _rn = 1
