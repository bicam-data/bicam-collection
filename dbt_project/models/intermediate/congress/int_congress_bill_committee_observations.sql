{{ config(materialized="table", tags=["bills", "committees", "congress", "child_state"]) }}

/* Resolver-selected bill-to-committee observations with source lineage. */
with committee_responses as (
    {{ resolve_child('bill_detail', 'bill_committees', 'bills') }}
), unnested as (
    select
        committee_responses.raw_id as source_raw_id,
        committee_responses.raw_response_version_id as source_raw_response_version_id,
        committee_responses.latest_crawl_run_id as source_crawl_run_id,
        committee_responses.parent_raw_id as bill_detail_raw_id,
        committee_responses.parent_endpoint_url as bill_detail_endpoint_url,
        committee_responses.endpoint_url as source_endpoint_url,
        committee_responses.child_fetched_at as source_fetched_at,
        committee_responses.page_offset as source_page_offset,
        committee_responses.is_complete_generation as source_is_complete_generation,
        committee_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        committee_responses.generation_page_count as source_generation_page_count,
        committee_responses.generation_expected_count as source_generation_expected_count,
        committee_responses.generation_finished_at as source_generation_finished_at,
        committees.value as committee_json,
        committees.array_ordinal as source_ordinal,
        bills.bill_id
    from committee_responses
    join {{ ref('stg_congress__bill_detail') }} as bills
      on committee_responses.parent_raw_id = bills.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(committee_responses.response -> 'committees', '[]'::jsonb)
    ) with ordinality as committees(value, array_ordinal)
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
        nullif(committee_json #>> '{systemCode}', '') as committee_code,
        nullif(committee_json #>> '{name}', '') as committee_name
    from unnested
)
select *
from parsed
