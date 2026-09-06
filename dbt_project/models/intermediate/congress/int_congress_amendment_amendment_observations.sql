{{ config(materialized="table", tags=["amendments", "congress", "child_state"]) }}

/* Resolver-selected amendment-to-amendment observations with source lineage. */
with amendment_responses as (
    {{ resolve_child('amendment_detail', 'amendment_amendments_to_amendment', 'amendments') }}
), unnested as (
    select
        amendment_responses.raw_id as source_raw_id,
        amendment_responses.raw_response_version_id as source_raw_response_version_id,
        amendment_responses.latest_crawl_run_id as source_crawl_run_id,
        amendment_responses.parent_raw_id as amendment_detail_raw_id,
        amendment_responses.parent_endpoint_url as amendment_detail_endpoint_url,
        amendment_responses.endpoint_url as source_endpoint_url,
        amendment_responses.child_fetched_at as source_fetched_at,
        amendment_responses.page_offset as source_page_offset,
        amendment_responses.is_complete_generation as source_is_complete_generation,
        amendment_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        amendment_responses.generation_page_count as source_generation_page_count,
        amendment_responses.generation_expected_count as source_generation_expected_count,
        amendment_responses.generation_finished_at as source_generation_finished_at,
        amendment.value as amendment_json,
        amendment.array_ordinal as source_ordinal
    from amendment_responses
    cross join lateral jsonb_array_elements(
        coalesce(amendment_responses.response -> 'amendments', '[]'::jsonb)
    ) with ordinality as amendment(value, array_ordinal)
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
    {{ json_int('amendment_json', 'congress') }} as congress,
    {{ json_text('amendment_json', 'number') }} as amendment_number,
    {{ json_text('amendment_json', 'type') }} as amendment_type,
    {{ json_timestamptz('amendment_json', 'updateDate') }} as update_date,
    {{ json_text('amendment_json', 'url') }} as amendment_url
from unnested
