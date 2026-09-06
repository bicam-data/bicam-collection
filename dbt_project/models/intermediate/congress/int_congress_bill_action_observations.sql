{{ config(materialized="table", tags=["bills", "congress", "child_state"]) }}

/*
   Select bill action arrays once after child-state reconstruction.  Downstream
   action, committee-edge, and recorded-vote projections all consume this
   occurrence relation so they cannot independently choose different raw
   windows for the same bill.
*/
with action_responses as (
    {{ resolve_child('bill_detail', 'bill_actions', 'bills') }}
), unnested as (
    select
        action_responses.raw_id as source_raw_id,
        action_responses.raw_response_version_id as source_raw_response_version_id,
        action_responses.latest_crawl_run_id as source_crawl_run_id,
        action_responses.parent_raw_id as bill_detail_raw_id,
        action_responses.parent_endpoint_url as bill_detail_endpoint_url,
        action_responses.endpoint_url as source_endpoint_url,
        action_responses.child_fetched_at as source_fetched_at,
        action_responses.page_offset as source_page_offset,
        action_responses.is_complete_generation as source_is_complete_generation,
        action_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        action_responses.generation_page_count as source_generation_page_count,
        action_responses.generation_expected_count as source_generation_expected_count,
        action_responses.generation_finished_at as source_generation_finished_at,
        actions.value as action_json,
        actions.array_ordinal as source_ordinal,
        bills.bill_id
    from action_responses
    join {{ ref('stg_congress__bill_detail') }} as bills
      on action_responses.parent_raw_id = bills.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(action_responses.response -> 'actions', '[]'::jsonb)
    ) with ordinality as actions(value, array_ordinal)
)
select
    source_raw_id,
    source_raw_response_version_id,
    source_crawl_run_id,
    bill_detail_raw_id,
    bill_detail_endpoint_url,
    source_endpoint_url,
    source_fetched_at,
    source_page_offset,
    source_is_complete_generation,
    source_generation_crawl_run_id,
    source_generation_page_count,
    source_generation_expected_count,
    source_generation_finished_at,
    source_ordinal,
    bill_id,
    action_json
from unnested
