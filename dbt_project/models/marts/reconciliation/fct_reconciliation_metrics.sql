{{ config(tags=["reconciliation", "operations", "quality"]) }}

with match_sets as (
    select
        'bill' as entity,
        match_id::text as match_id,
        decision,
        congress_source_record_id,
        govinfo_source_record_id,
        title_agrees
    from {{ ref('fct_bill_matches') }}

    union all

    select
        'committee_report' as entity,
        match_id::text as match_id,
        decision,
        congress_source_record_id,
        govinfo_source_record_id,
        null::boolean as title_agrees
    from {{ ref('fct_committee_report_matches') }}

    union all

    select
        'hearing' as entity,
        match_id::text as match_id,
        decision,
        congress_source_record_id,
        govinfo_source_record_id,
        null::boolean as title_agrees
    from {{ ref('fct_hearing_matches') }}
),
entities as (
    select distinct entity from match_sets
),
thresholds as (
    select * from {{ ref('reconciliation_quality_thresholds') }}
),
exceptions as (
    select
        entity,
        match_id::text as match_id,
        exception_status,
        review_disposition
    from {{ ref('fct_reconciliation_exceptions') }}
),
classified as (
    select
        match_sets.*,
        thresholds.authoritative_source,
        thresholds.contract_version,
        thresholds.max_ambiguous_rate,
        thresholds.min_authoritative_match_rate,
        thresholds.min_title_agreement_rate,
        thresholds.entity is not null as threshold_present,
        case thresholds.authoritative_source
            when 'congress' then match_sets.congress_source_record_id is not null
            when 'govinfo' then match_sets.govinfo_source_record_id is not null
            else false
        end as is_authoritative,
        coalesce(
            exceptions.exception_status = 'reviewed'
            and exceptions.review_disposition = 'confirmed_source_gap',
            false
        ) as is_excused_source_gap,
        coalesce(exceptions.exception_status = 'open', false) as is_open_exception,
        coalesce(exceptions.exception_status = 'reviewed', false) as is_reviewed_exception
    from match_sets
    left join thresholds on thresholds.entity = match_sets.entity
    left join exceptions
      on exceptions.entity = match_sets.entity
     and exceptions.match_id = match_sets.match_id
),
aggregated as (
    select
        entities.entity,
        max(classified.authoritative_source) as authoritative_source,
        max(classified.contract_version) as contract_version,
        coalesce(bool_or(classified.threshold_present), false) as threshold_present,
        max(classified.max_ambiguous_rate) as max_ambiguous_rate,
        max(classified.min_authoritative_match_rate) as min_authoritative_match_rate,
        max(classified.min_title_agreement_rate) as min_title_agreement_rate,
        count(classified.match_id) as candidate_count,
        count(*) filter (where classified.decision = 'accepted') as accepted_count,
        count(*) filter (where classified.decision = 'rejected') as rejected_count,
        count(*) filter (where classified.decision = 'ambiguous') as ambiguous_count,
        count(*) filter (where classified.decision = 'unmatched') as unmatched_count,
        count(*) filter (where classified.is_authoritative) as authoritative_record_count,
        count(*) filter (
            where classified.is_authoritative
              and classified.decision = 'accepted'
        ) as authoritative_accepted_count,
        count(*) filter (
            where classified.is_authoritative
              and classified.is_excused_source_gap
        ) as authoritative_source_gap_count,
        count(*) filter (
            where classified.is_authoritative
              and not classified.is_excused_source_gap
        ) as eligible_authoritative_record_count,
        count(*) filter (
            where classified.is_authoritative
              and not classified.is_excused_source_gap
              and classified.decision = 'accepted'
        ) as eligible_authoritative_accepted_count,
        count(*) filter (where classified.is_open_exception) as open_exception_count,
        count(*) filter (where classified.is_reviewed_exception) as reviewed_exception_count,
        count(*) filter (
            where classified.is_authoritative
              and not classified.is_excused_source_gap
              and classified.decision = 'unmatched'
        ) as unexcused_unmatched_count,
        count(*) filter (
            where classified.is_authoritative
              and not classified.is_excused_source_gap
              and classified.decision = 'rejected'
        ) as unexcused_rejected_count,
        count(*) filter (
            where classified.is_authoritative
              and not classified.is_excused_source_gap
              and classified.decision = 'ambiguous'
        ) as unexcused_ambiguous_count,
        count(*) filter (
            where classified.is_authoritative
              and classified.decision = 'accepted'
              and classified.title_agrees is not null
        ) as title_comparable_accepted_count,
        count(*) filter (
            where classified.is_authoritative
              and classified.decision = 'accepted'
              and classified.title_agrees
        ) as title_agreement_count
    from entities
    left join classified on classified.entity = entities.entity
    group by entities.entity
),
rates as (
    select
        aggregated.*,
        coalesce(ambiguous_count::numeric / nullif(candidate_count, 0), 0) as ambiguous_rate,
        coalesce(
            authoritative_accepted_count::numeric / nullif(authoritative_record_count, 0),
            0
        ) as authoritative_match_rate,
        case
            when eligible_authoritative_record_count = 0 then 1::numeric
            else eligible_authoritative_accepted_count::numeric / eligible_authoritative_record_count
        end as eligible_authoritative_match_rate,
        case
            when title_comparable_accepted_count = 0 then null::numeric
            else title_agreement_count::numeric / title_comparable_accepted_count
        end as title_agreement_rate
    from aggregated
)

select
    {{ hash_to_uuid("rates.entity || '|' || coalesce(rates.contract_version, 'missing')") }} as metric_id,
    rates.*,
    case
        when not threshold_present then 'fail_missing_threshold'
        when max_ambiguous_rate is null
          or min_authoritative_match_rate is null
          or max_ambiguous_rate < 0
          or max_ambiguous_rate > 1
          or min_authoritative_match_rate < 0
          or min_authoritative_match_rate > 1
          or min_title_agreement_rate is null
          or min_title_agreement_rate < 0
          or min_title_agreement_rate > 1
            then 'fail_invalid_threshold'
        when open_exception_count > 0 then 'fail_exceptions'
        when unexcused_unmatched_count > 0
          or unexcused_rejected_count > 0
            then 'fail_unexcused_records'
        when unexcused_ambiguous_count > 0
          or ambiguous_rate > max_ambiguous_rate
            then 'fail_ambiguity'
        when eligible_authoritative_match_rate < min_authoritative_match_rate
            then 'fail_match_rate'
        when min_title_agreement_rate > 0
          and (title_agreement_rate is null or title_agreement_rate < min_title_agreement_rate)
            then 'fail_title_corroboration'
        else 'pass'
    end as quality_status
from rates
