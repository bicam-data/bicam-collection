{{ config(materialized="table", tags=["nominations", "congress", "child_state"]) }}

/* Resolver-selected nomination action observations shared by action and
   action-committee public projections. */
with action_responses as (
    {{ resolve_child('nomination_detail', 'nomination_actions', 'nominations') }}
), unnested as (
    select
        action_responses.raw_id as source_raw_id,
        action_responses.raw_response_version_id as source_raw_response_version_id,
        action_responses.latest_crawl_run_id as source_crawl_run_id,
        action_responses.parent_raw_id as nomination_detail_raw_id,
        action_responses.parent_endpoint_url as nomination_detail_endpoint_url,
        action_responses.endpoint_url as source_endpoint_url,
        action_responses.child_fetched_at as source_fetched_at,
        action_responses.page_offset as source_page_offset,
        action_responses.is_complete_generation as source_is_complete_generation,
        action_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        action_responses.generation_page_count as source_generation_page_count,
        action_responses.generation_expected_count as source_generation_expected_count,
        action_responses.generation_finished_at as source_generation_finished_at,
        substring(action_responses.parent_endpoint_url from '/nomination/([0-9]+)/')::integer as congress,
        substring(action_responses.parent_endpoint_url from '/nomination/[0-9]+/([0-9]+)') as nomination_number,
        lpad(coalesce(
            substring(action_responses.parent_endpoint_url from '/nomination/[0-9]+/[0-9]+-([0-9]+)'),
            '00'
        ), 2, '0') as nomination_part,
        action.value as action_json,
        action.array_ordinal as source_ordinal
    from action_responses
    cross join lateral jsonb_array_elements(
        coalesce(action_responses.response -> 'actions', '[]'::jsonb)
    ) with ordinality as action(value, array_ordinal)
), parsed as (
    select
        source_raw_id,
        source_raw_response_version_id,
        source_crawl_run_id,
        nomination_detail_raw_id,
        nomination_detail_endpoint_url,
        source_endpoint_url,
        source_fetched_at,
        source_page_offset,
        source_is_complete_generation,
        source_generation_crawl_run_id,
        source_generation_page_count,
        source_generation_expected_count,
        source_generation_finished_at,
        source_ordinal,
        'pn' || nomination_number || '-' || congress::text as nomination_id,
        'pn' || nomination_number || '-' || nomination_part || '-' || congress::text as nomination_part_id,
        {{ hash_to_uuid(
            "'pn' || " ~ "nomination_number" ~ " || '-' || " ~ "nomination_part" ~ " || '-' || " ~ "congress::text" ~ " || '|' || action_json::text"
        ) }} as action_id,
        {{ json_text('action_json', 'actionCode') }} as action_code,
        {{ json_text('action_json', 'type') }} as action_type,
        {{ json_date('action_json', 'actionDate') }} as action_date,
        {{ json_text('action_json', 'text') }} as text,
        action_json
    from unnested
)
select *
from parsed
