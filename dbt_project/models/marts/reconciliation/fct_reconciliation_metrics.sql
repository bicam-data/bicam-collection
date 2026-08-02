{{ config(tags=["reconciliation", "operations", "quality"]) }}

with match_sets as (
    select
        'bill' as entity,
        decision,
        congress_source_record_id,
        govinfo_source_record_id
    from {{ ref('fct_bill_matches') }}
    union all
    select
        'committee_report' as entity,
        decision,
        congress_source_record_id,
        govinfo_source_record_id
    from {{ ref('fct_committee_report_matches') }}
),
aggregated as (
    select
        entity,
        count(*) as candidate_count,
        count(*) filter (where decision = 'accepted') as accepted_count,
        count(*) filter (where decision = 'rejected') as rejected_count,
        count(*) filter (where decision = 'ambiguous') as ambiguous_count,
        count(*) filter (where decision = 'unmatched') as unmatched_count,
        count(*) filter (where congress_source_record_id is not null) as authoritative_record_count,
        count(*) filter (where congress_source_record_id is not null and decision = 'accepted') as authoritative_accepted_count
    from match_sets
    group by 1
),
thresholds as (
    select * from {{ ref('reconciliation_quality_thresholds') }}
)

select
    {{ hash_to_uuid("aggregated.entity || '|' || thresholds.contract_version") }} as metric_id,
    aggregated.entity,
    thresholds.authoritative_source,
    thresholds.contract_version,
    candidate_count,
    accepted_count,
    rejected_count,
    ambiguous_count,
    unmatched_count,
    authoritative_record_count,
    authoritative_accepted_count,
    coalesce(ambiguous_count::numeric / nullif(candidate_count, 0), 0) as ambiguous_rate,
    coalesce(authoritative_accepted_count::numeric / nullif(authoritative_record_count, 0), 0) as authoritative_match_rate,
    thresholds.max_ambiguous_rate,
    thresholds.min_authoritative_match_rate,
    case
        when coalesce(ambiguous_count::numeric / nullif(candidate_count, 0), 0) > thresholds.max_ambiguous_rate then 'fail_ambiguity'
        when coalesce(authoritative_accepted_count::numeric / nullif(authoritative_record_count, 0), 0) < thresholds.min_authoritative_match_rate then 'fail_match_rate'
        else 'pass'
    end as quality_status
from aggregated
join thresholds on thresholds.entity = aggregated.entity
