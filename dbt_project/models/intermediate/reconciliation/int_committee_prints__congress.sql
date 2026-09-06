{{ config(tags=["committee_print_reconciliation", "committee_prints", "intermediate"]) }}

select
    'congress:' || print_id as source_record_id,
    'congress' as source_name,
    raw_id as source_raw_id,
    raw_response_version_id as source_raw_response_version_id,
    endpoint_url as source_endpoint_url,
    lower(print_id) as canonical_print_id,
    print_type,
    print_jacketnumber,
    congress,
    {{ normalize_chamber('chamber') }} as chamber,
    title,
    {{ normalize_match_text('title') }} as normalized_title,
    citation,
    texts_count,
    updated_at as source_updated_at
from {{ ref('stg_congress__committee_print_detail') }}
where print_id is not null
