{{ config(
    materialized='table',
    tags=['operations', 'quality', 'source_gap', 'child_state'],
    indexes=[
      {'columns': ['child_collection']},
      {'columns': ['canonical_id'], 'unique': true}
    ]
) }}

/*
   Selected child pages that cannot be proven to belong to a complete
   generation are actionable source gaps. Keep this relation separate from
   cross-source reconciliation exceptions: it is evidence about collection
   completeness, not evidence that a source record is missing.

   The durable child-selection relation already contains the resolver's
   endpoint, generation, pagination, fetch, and raw-version evidence. Reading
   it directly avoids rebuilding every large observation expansion merely to
   identify an incomplete page. The observation relations remain responsible
   for public child records; this queue only needs their shared transport
   evidence.
*/
with selected_incomplete as (
    /* The selection relation also retains incomplete empty-array payloads,
       which have no unnested observation row to report. Restrict this queue to
       the same in-scope child families that were previously represented by
       observation relations; paused House families remain out of scope. */
    select
        case
            when node_type = 'nomination_nominee_detail' then 'nominees'
            when node_type in ('member_sponsored_legislation', 'member_cosponsored_legislation')
                then 'member_legislation'
            when node_type = 'nomination_committees' then 'nomination_committeeactivities'
            when node_type = 'amendment_texts' then 'amendment_texts'
            else node_type
        end::text as child_collection,
        endpoint_url as source_endpoint_url,
        child_fetched_at as source_fetched_at
    from {{ ref('int_congress_child_response_selections') }}
    where is_complete_generation is false
      and node_type in (
          'amendment_actions',
          'amendment_amendments_to_amendment',
          'amendment_cosponsors',
          'amendment_texts',
          'bill_actions',
          'bill_amendments',
          'bill_committees',
          'bill_cosponsors',
          'bill_relatedbills',
          'bill_subjects',
          'bill_summaries',
          'bill_titles',
          'bill_text',
          'committee_bills',
          'committee_communications',
          'committee_nominations',
          'committee_print_texts',
          'committee_reports',
          'committee_report_texts',
          'member_cosponsored_legislation',
          'member_sponsored_legislation',
          'nomination_actions',
          'nomination_committees',
          'nomination_hearings',
          'nomination_nominee_detail',
          'treaty_actions',
          'treaty_committees'
      )
), all_incomplete as (
    select child_collection, source_endpoint_url, source_fetched_at
    from selected_incomplete
    where child_collection is not null
), grouped as (
    select
        child_collection,
        {{ congress_child_logical_endpoint('source_endpoint_url') }} as source_logical_endpoint,
        (array_agg(source_endpoint_url order by source_fetched_at desc nulls last, source_endpoint_url desc))[1]
            as latest_endpoint_url,
        max(source_fetched_at) as latest_fetch_at
    from all_incomplete
    where source_endpoint_url is not null
    group by child_collection, source_logical_endpoint
)
select
    {{ hash_to_uuid("'child_state|' || child_collection || '|' || source_logical_endpoint") }} as queue_id,
    'child_state'::text as entity,
    child_collection || '|' || source_logical_endpoint as canonical_id,
    child_collection,
    'congress'::text as missing_source,
    null::integer as congress_number,
    'incomplete_generation'::text as match_decision,
    'needs_source_refresh'::text as review_disposition,
    'open'::text as exception_status,
    'incomplete'::text as latest_fetch_outcome,
    200::integer as latest_fetch_http_status,
    latest_fetch_at,
    latest_endpoint_url as endpoint_url,
    'incomplete_child_generation'::text as decision_rule,
    'Selected child response is not part of a proven complete generation.'::text as review_reason,
    null::timestamptz as expires_at,
    null::text as reviewer,
    null::timestamptz as reviewed_at,
    'child-state-v1'::text as contract_version
from grouped
