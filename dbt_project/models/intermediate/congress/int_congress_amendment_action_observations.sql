{{ config(materialized="table", tags=["amendments", "congress", "child_state"]) }}

/* Shared resolver-selected amendment action occurrences. */
with action_responses as (
    {{ resolve_child('amendment_detail', 'amendment_actions', 'amendments') }}
), unnested as (
    select
        action_responses.raw_id as source_raw_id,
        action_responses.raw_response_version_id as source_raw_response_version_id,
        action_responses.latest_crawl_run_id as source_crawl_run_id,
        action_responses.parent_raw_id as amendment_detail_raw_id,
        action_responses.parent_endpoint_url as amendment_detail_endpoint_url,
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
        amendments.amendment_id
    from action_responses
    join {{ ref('stg_congress__amendment_detail') }} as amendments
      on action_responses.parent_raw_id = amendments.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(action_responses.response -> 'actions', '[]'::jsonb)
    ) with ordinality as actions(value, array_ordinal)
)
select
    source_raw_id,
    source_raw_response_version_id,
    source_crawl_run_id,
    amendment_detail_raw_id,
    amendment_detail_endpoint_url,
    source_endpoint_url,
    source_fetched_at,
    source_page_offset,
    source_is_complete_generation,
    source_generation_crawl_run_id,
    source_generation_page_count,
    source_generation_expected_count,
    source_generation_finished_at,
    source_ordinal,
    amendment_id,
    action_json
from unnested
