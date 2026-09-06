{{ config(tags=["bills", "committee_reports", "congress", "intermediate", "reconciliation", "committee_report_reconciliation"]) }}

with bill_side as (
    select distinct
        bill_id,
        bill_type,
        bill_number,
        bill_congress,
        report_id,
        report_part_id,
        report_type,
        report_number,
        report_part,
        congress,
        is_errata,
        'bill_committee_reports' as evidence_direction,
        raw_id::text as source_raw_id,
        raw_response_version_id::text as source_raw_response_version_id,
        endpoint_url as source_endpoint_url
    from {{ ref('stg_congress__bill_committeereports') }}
),
report_side as (
    select distinct
        report_bills.bill_id,
        report_bills.bill_type,
        report_bills.bill_number::text,
        report_bills.congress as bill_congress,
        report_bills.report_id,
        report_bills.report_part_id,
        report_detail.report_type,
        report_detail.report_number,
        report_detail.report_part,
        report_detail.congress,
        report_detail.is_errata,
        'committee_report_associated_bills' as evidence_direction,
        report_detail.raw_id::text as source_raw_id,
        report_detail.raw_response_version_id::text as source_raw_response_version_id,
        report_detail.endpoint_url as source_endpoint_url
    from {{ ref('stg_congress__committee_report_bills') }} as report_bills
    join {{ ref('stg_congress__committee_report_detail') }} as report_detail
      on report_detail.report_part_id = report_bills.report_part_id
),
evidence as (
    select * from bill_side
    union all
    select * from report_side
)

select
    {{ hash_to_uuid("bill_id || '|' || report_part_id") }} as link_id,
    bill_id,
    {{ bill_canonical_id('bill_type', 'bill_number', 'bill_congress') }} as canonical_bill_id,
    report_id,
    report_part_id,
    {{ committee_report_canonical_id(
        'report_type', 'report_number', 'congress', 'report_part', 'is_errata'
    ) }} as canonical_report_id,
    report_type,
    report_number,
    report_part,
    congress,
    is_errata,
    bool_or(evidence_direction = 'bill_committee_reports') as from_bill_side,
    bool_or(evidence_direction = 'committee_report_associated_bills') as from_report_side,
    array_agg(distinct evidence_direction order by evidence_direction) as evidence_directions,
    count(*)::integer as evidence_count,
    array_agg(distinct source_raw_id order by source_raw_id) as source_raw_ids,
    array_agg(distinct source_raw_response_version_id order by source_raw_response_version_id)
        as source_raw_response_version_ids,
    array_agg(distinct source_endpoint_url order by source_endpoint_url) as source_endpoint_urls
from evidence
group by
    bill_id,
    bill_type,
    bill_number,
    bill_congress,
    report_id,
    report_part_id,
    report_type,
    report_number,
    report_part,
    congress,
    is_errata
