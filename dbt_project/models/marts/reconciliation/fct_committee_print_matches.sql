{{ config(tags=["committee_print_reconciliation", "committee_prints", "reconciliation", "marts"]) }}

select
    candidate_id as match_id,
    canonical_print_id,
    congress_source_record_id,
    govinfo_source_record_id,
    congress_raw_id,
    congress_raw_response_version_id,
    congress_endpoint_url,
    govinfo_raw_id,
    govinfo_raw_response_version_id,
    govinfo_endpoint_url,
    govinfo_package_id,
    candidate_rule as decision_rule,
    case
        when has_exact_identity then 'accepted'
        else 'unmatched'
    end as decision,
    candidate_confidence,
    title_agrees,
    congress_print_id,
    govinfo_print_id,
    congress_print_type,
    govinfo_print_type,
    congress_print_jacketnumber,
    govinfo_print_jacketnumber,
    congress_congress,
    govinfo_congress,
    congress_chamber,
    govinfo_chamber,
    congress_title,
    govinfo_title,
    govinfo_identity_status
from {{ ref('int_committee_print_candidates') }}
