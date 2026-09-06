{{ config(materialized="table", tags=["bills", "amendments", "congress", "child_state"]) }}

/* Resolver-selected bill-to-amendment observations with source lineage. */
with amendment_responses as (
    {{ resolve_child('bill_detail', 'bill_amendments', 'bills') }}
), unnested as (
    select
        amendment_responses.raw_id as source_raw_id,
        amendment_responses.raw_response_version_id as source_raw_response_version_id,
        amendment_responses.latest_crawl_run_id as source_crawl_run_id,
        amendment_responses.parent_raw_id as bill_detail_raw_id,
        amendment_responses.parent_endpoint_url as bill_detail_endpoint_url,
        amendment_responses.endpoint_url as source_endpoint_url,
        amendment_responses.child_fetched_at as source_fetched_at,
        amendment_responses.page_offset as source_page_offset,
        amendment_responses.is_complete_generation as source_is_complete_generation,
        amendment_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        amendment_responses.generation_page_count as source_generation_page_count,
        amendment_responses.generation_expected_count as source_generation_expected_count,
        amendment_responses.generation_finished_at as source_generation_finished_at,
        amendments.value as amendment_json,
        amendments.array_ordinal as source_ordinal,
        bills.bill_id
    from amendment_responses
    join {{ ref('stg_congress__bill_detail') }} as bills
      on amendment_responses.parent_raw_id = bills.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(amendment_responses.response -> 'amendments', '[]'::jsonb)
    ) with ordinality as amendments(value, array_ordinal)
), parsed as (
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
        {{ build_custom_id([
            "lower(nullif(amendment_json #>> '{type}', ''))",
            "amendment_json #>> '{number}'"
        ]) }} || '-' || coalesce(amendment_json #>> '{congress}', '') as amendment_id,
        lower(nullif(amendment_json #>> '{type}', '')) as amendment_type,
        (nullif(amendment_json #>> '{number}', ''))::integer as amendment_number,
        (nullif(amendment_json #>> '{congress}', ''))::integer as congress
    from unnested
)
select *
from parsed
