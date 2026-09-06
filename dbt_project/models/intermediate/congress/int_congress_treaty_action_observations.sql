{{ config(materialized="table", tags=["treaties", "congress", "child_state"]) }}

/* Resolver-selected treaty action occurrences with source lineage. */
with action_responses as (
    {{ resolve_child('treaty_detail', 'treaty_actions', 'treaties') }}
), unnested as (
    select
        action_responses.raw_id as source_raw_id,
        action_responses.raw_response_version_id as source_raw_response_version_id,
        action_responses.latest_crawl_run_id as source_crawl_run_id,
        action_responses.parent_raw_id as treaty_detail_raw_id,
        action_responses.parent_endpoint_url as treaty_detail_endpoint_url,
        action_responses.endpoint_url as source_endpoint_url,
        action_responses.child_fetched_at as source_fetched_at,
        action_responses.page_offset as source_page_offset,
        action_responses.is_complete_generation as source_is_complete_generation,
        action_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        action_responses.generation_page_count as source_generation_page_count,
        action_responses.generation_expected_count as source_generation_expected_count,
        action_responses.generation_finished_at as source_generation_finished_at,
        action_item.value as action_json,
        action_item.array_ordinal as source_ordinal,
        substring(action_responses.endpoint_url from '/treaty/([0-9]+)/')::integer as congress,
        substring(action_responses.endpoint_url from '/treaty/[0-9]+/([0-9]+)')::integer as treaty_number,
        substring(action_responses.endpoint_url from '/treaty/[0-9]+/[0-9]+/([^/?]+)/actions') as suffix
    from action_responses
    cross join lateral jsonb_array_elements(
        coalesce(action_responses.response -> 'actions', '[]'::jsonb)
    ) with ordinality as action_item(value, array_ordinal)
), parsed as (
    select
        source_raw_id,
        source_raw_response_version_id,
        source_crawl_run_id,
        treaty_detail_raw_id,
        treaty_detail_endpoint_url,
        source_endpoint_url,
        source_fetched_at,
        source_page_offset,
        source_is_complete_generation,
        source_generation_crawl_run_id,
        source_generation_page_count,
        source_generation_expected_count,
        source_generation_finished_at,
        source_ordinal,
        'td'
        || congress::text
        || '-'
        || treaty_number::text as treaty_id,
        'td'
        || congress::text
        || '-'
        || treaty_number::text
        || coalesce(suffix, '') as treaty_part_id,
        {{ hash_to_uuid(
            "'td' || congress::text || '-' || treaty_number::text || coalesce(suffix, '') || '|' || action_json::text"
        ) }} as action_id,
        {{ json_text('action_json', 'actionCode') }} as action_code,
        {{ json_date('action_json', 'actionDate') }} as action_date,
        {{ json_text('action_json', 'text') }} as text,
        {{ json_text('action_json', 'type') }} as action_type,
        {{ json_text('action_json', 'committee.systemCode') }} as committee_id,
        {{ json_text('action_json', 'committee.name') }} as committee_name,
        action_json
    from unnested
)
select *
from parsed
