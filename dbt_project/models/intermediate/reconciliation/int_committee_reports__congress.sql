{{ config(tags=["committee_report_reconciliation", "intermediate"]) }}

/*
   Congress.gov exposes committee-report identity in two directions.  The
   standalone report endpoint is the descriptive authority when available,
   but a bill's ``committeeReports`` child can be the only observed Congress
   record for a report that GovInfo has already published.  Keep both source
   observations here.  The candidate model chooses one deterministic Congress
   representative per canonical report so the full outer join cannot multiply
   matches, while this relation preserves the evidence needed to explain the
   source-side union.
*/
with report_detail_records as (
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
        updated_at as source_updated_at,
        0 as source_priority
    from {{ ref('stg_congress__committee_report_detail') }}
), bill_report_records as (
    select distinct
        'congress:bill_committee_report:' || report_part_id || ':' || raw_id::text as source_record_id,
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
        null::text as title,
        null::text as normalized_title,
        null::text as chamber,
        null::timestamptz as issued_at,
        null::timestamptz as source_updated_at,
        1 as source_priority
    from {{ ref('stg_congress__bill_committeereports') }}
    where report_part_id is not null
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
from report_detail_records
union all
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
from bill_report_records
