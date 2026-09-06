{{ config(materialized="table", tags=["treaties", "congress", "child_state"]) }}

/* Resolver-selected treaty committee observations.  Downstream committee
   and activity projections share this selected child state rather than each
   re-expanding the raw response independently. */
with committee_responses as (
    {{ resolve_child('treaty_detail', 'treaty_committees', 'treaties') }}
), unnested as (
    select
        committee_responses.raw_id as source_raw_id,
        committee_responses.raw_response_version_id as source_raw_response_version_id,
        committee_responses.latest_crawl_run_id as source_crawl_run_id,
        committee_responses.parent_raw_id as treaty_detail_raw_id,
        committee_responses.parent_endpoint_url as treaty_detail_endpoint_url,
        committee_responses.endpoint_url as source_endpoint_url,
        committee_responses.child_fetched_at as source_fetched_at,
        committee_responses.page_offset as source_page_offset,
        committee_responses.is_complete_generation as source_is_complete_generation,
        committee_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        committee_responses.generation_page_count as source_generation_page_count,
        committee_responses.generation_expected_count as source_generation_expected_count,
        committee_responses.generation_finished_at as source_generation_finished_at,
        substring(committee_responses.endpoint_url from '/treaty/([0-9]+)/')::integer as congress,
        substring(committee_responses.endpoint_url from '/treaty/[0-9]+/([0-9]+)')::integer as treaty_number,
        committee.value as committee_json,
        committee.array_ordinal as source_ordinal
    from committee_responses
    cross join lateral jsonb_array_elements(
        coalesce(committee_responses.response #> '{treatyCommittees}', '[]'::jsonb)
    ) with ordinality as committee(value, array_ordinal)
)
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
    'td' || congress::text || '-' || treaty_number::text as treaty_id,
    'td' || congress::text || '-' || treaty_number::text as treaty_part_id,
    committee_json
from unnested
