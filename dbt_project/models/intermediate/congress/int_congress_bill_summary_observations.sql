{{ config(materialized="table", tags=["bills", "congress", "child_state"]) }}

/* Resolver-selected bill-summary occurrences with source lineage. */
with summary_responses as (
    {{ resolve_child('bill_detail', 'bill_summaries', 'bills') }}
), unnested as (
    select
        summary_responses.raw_id as source_raw_id,
        summary_responses.raw_response_version_id as source_raw_response_version_id,
        summary_responses.latest_crawl_run_id as source_crawl_run_id,
        summary_responses.parent_raw_id as bill_detail_raw_id,
        summary_responses.parent_endpoint_url as bill_detail_endpoint_url,
        summary_responses.endpoint_url as source_endpoint_url,
        summary_responses.child_fetched_at as source_fetched_at,
        summary_responses.page_offset as source_page_offset,
        summary_responses.is_complete_generation as source_is_complete_generation,
        summary_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        summary_responses.generation_page_count as source_generation_page_count,
        summary_responses.generation_expected_count as source_generation_expected_count,
        summary_responses.generation_finished_at as source_generation_finished_at,
        summaries.value as summary_json,
        summaries.array_ordinal as source_ordinal,
        bills.bill_id
    from summary_responses
    join {{ ref('stg_congress__bill_detail') }} as bills
      on summary_responses.parent_raw_id = bills.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(summary_responses.response -> 'summaries', '[]'::jsonb)
    ) with ordinality as summaries(value, array_ordinal)
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
    (nullif(summary_json #>> '{actionDate}', ''))::date as action_date,
    nullif(summary_json #>> '{actionDesc}', '') as action_desc,
    nullif(summary_json #>> '{text}', '') as text,
    nullif(summary_json #>> '{versionCode}', '') as version_code,
    (nullif(summary_json #>> '{updateDate}', ''))::timestamptz as updated_at
from unnested
