{{ config(tags=["bill_reconciliation", "intermediate"]) }}

select
    'congress:' || bill_id as source_record_id,
    'congress' as source_name,
    {{ bill_canonical_id('bill_type', 'bill_number', 'congress') }} as canonical_bill_id,
    bill_type,
    bill_number,
    congress,
    title,
    {{ normalize_match_text('title') }} as normalized_title,
    origin_chamber,
    introduced_at,
    updated_at as source_updated_at
from {{ ref('stg_congress__bill_detail') }}
