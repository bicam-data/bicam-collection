{{ config(tags=["committee_report_reconciliation", "marts"]) }}

with candidates as (
    select * from {{ ref('int_committee_report_candidates') }}
),
overrides as (
    select
        nullif(congress_source_record_id::text, '') as congress_source_record_id,
        nullif(govinfo_source_record_id::text, '') as govinfo_source_record_id,
        nullif(decision::text, '') as decision,
        nullif(reviewer::text, '') as reviewer,
        nullif(reviewed_at::text, '')::timestamptz as reviewed_at,
        nullif(reason::text, '') as reason
    from {{ ref('committee_report_match_overrides') }}
),
resolved as (
    select
        candidates.*,
        overrides.decision as override_decision,
        overrides.reviewer,
        overrides.reviewed_at,
        overrides.reason as review_reason
    from candidates
    left join overrides
      on candidates.congress_source_record_id = overrides.congress_source_record_id
     and candidates.govinfo_source_record_id = overrides.govinfo_source_record_id
)

select
    candidate_id as match_id,
    canonical_report_id,
    congress_source_record_id,
    govinfo_source_record_id,
    coalesce(
        override_decision,
        case
            when congress_source_record_id is not null and govinfo_source_record_id is not null then 'accepted'
            else 'unmatched'
        end
    ) as decision,
    case
        when override_decision is not null then 'reviewed_override'
        else candidate_rule
    end as decision_rule,
    title_agrees,
    chamber_agrees,
    congress_title,
    govinfo_title,
    reviewer,
    reviewed_at,
    review_reason
from resolved

