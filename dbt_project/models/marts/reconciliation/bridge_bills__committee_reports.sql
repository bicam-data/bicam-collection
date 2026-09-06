{{ config(
    materialized="table",
    tags=["bills", "committee_reports", "reconciliation", "bridge"]
) }}

/*
   The bill detail and committee-report detail endpoints expose the same
   relationship from opposite directions.  The intermediate model performs
   the source-neutral union; this durable mart gives consumers a stable
   release-facing relation while retaining which endpoint supplied evidence.
*/
select
    link_id,
    bill_id,
    canonical_bill_id,
    report_id,
    report_part_id,
    canonical_report_id,
    report_type,
    report_number,
    report_part,
    congress,
    is_errata,
    from_bill_side,
    from_report_side,
    evidence_directions,
    evidence_count,
    source_raw_ids,
    source_raw_response_version_ids,
    source_endpoint_urls
from {{ ref('int_committee_report_bill_links') }}
