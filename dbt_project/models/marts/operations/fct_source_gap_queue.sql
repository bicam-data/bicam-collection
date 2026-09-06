{{ config(
    materialized='table',
    tags=['operations', 'quality', 'source_gap'],
    indexes=[
      {'columns': ['queue_status']},
      {'columns': ['priority', 'next_action_at']},
      {'columns': ['entity', 'canonical_id'], 'unique': true}
    ]
) }}

/*
   Operational queue for source-side reconciliation gaps.  The exception fact
   remains the audit record; this relation is the small, actionable surface a
   scheduler or reviewer can poll.  It intentionally contains no guessed
   matches and keeps reviewed source gaps visible until their expiry/reprobe
   date.
*/
with queue_inputs as (
    select
        {{ hash_to_uuid("'source_gap|' || entity || '|' || canonical_id") }} as queue_id,
        entity,
        canonical_id,
        missing_source,
        congress_number,
        match_decision,
        review_disposition,
        exception_status,
        latest_fetch_outcome,
        latest_fetch_http_status,
        latest_fetch_at,
        coalesce(expected_missing_endpoint_url, congress_endpoint_url, govinfo_endpoint_url)
            as endpoint_url,
        decision_rule,
        review_reason,
        expires_at,
        reviewer,
        reviewed_at,
        contract_version
    from {{ ref('fct_reconciliation_exceptions') }}
    where missing_source in ('congress', 'govinfo')

    union all

    select
        queue_id,
        entity,
        canonical_id,
        missing_source,
        congress_number,
        match_decision,
        review_disposition,
        exception_status,
        latest_fetch_outcome,
        latest_fetch_http_status,
        latest_fetch_at,
        endpoint_url,
        decision_rule,
        review_reason,
        expires_at,
        reviewer,
        reviewed_at,
        contract_version
    from {{ ref('fct_child_state_gaps') }}
)
select
    queue_id,
    entity,
    canonical_id,
    missing_source,
    congress_number,
    match_decision,
    review_disposition,
    exception_status,
    latest_fetch_outcome,
    latest_fetch_http_status,
    latest_fetch_at,
    endpoint_url,
    decision_rule,
    review_reason,
    expires_at,
    case
        when review_disposition = 'needs_source_refresh' then 'reprobe'
        when review_disposition = 'confirmed_source_gap'
         and (expires_at is null or expires_at <= now()) then 'reprobe'
        when review_disposition = 'confirmed_source_gap' then 'reprobe_after_expiry'
        when exception_status = 'open' and latest_fetch_outcome in ('failed', 'not_attempted')
            then 'fetch_missing_source'
        when exception_status = 'open' then 'review_exception'
        else 'no_action'
    end as next_action,
    case
        when congress_number >= 118 then 'high'
        when congress_number >= 103 then 'normal'
        else 'low'
    end as priority,
    case
        when review_disposition = 'needs_source_refresh' then 'open'
        when review_disposition = 'confirmed_source_gap'
         and (expires_at is null or expires_at <= now()) then 'due_for_reprobe'
        when review_disposition = 'confirmed_source_gap' then 'deferred_source_gap'
        when exception_status = 'open' then 'open'
        else 'closed'
    end as queue_status,
    case
        when review_disposition = 'confirmed_source_gap' then expires_at
        when exception_status = 'open' then coalesce(latest_fetch_at, now())
        else null
    end as next_action_at,
    reviewer,
    reviewed_at,
    contract_version
from queue_inputs
