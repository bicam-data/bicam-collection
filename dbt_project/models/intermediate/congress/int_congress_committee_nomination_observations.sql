{{ config(materialized="table", tags=["committees", "congress", "child_state"]) }}

/* Resolver-selected committee nomination observations with source lineage. */
with nomination_responses as (
    {{ resolve_child('committee_detail', 'committee_nominations', 'committees') }}
), unnested as (
    select
        nomination_responses.raw_id as source_raw_id,
        nomination_responses.raw_response_version_id as source_raw_response_version_id,
        nomination_responses.latest_crawl_run_id as source_crawl_run_id,
        nomination_responses.parent_raw_id as committee_detail_raw_id,
        nomination_responses.parent_endpoint_url as committee_detail_endpoint_url,
        nomination_responses.endpoint_url as source_endpoint_url,
        nomination_responses.child_fetched_at as source_fetched_at,
        nomination_responses.page_offset as source_page_offset,
        nomination_responses.is_complete_generation as source_is_complete_generation,
        nomination_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        nomination_responses.generation_page_count as source_generation_page_count,
        nomination_responses.generation_expected_count as source_generation_expected_count,
        nomination_responses.generation_finished_at as source_generation_finished_at,
        nomination.value as nomination_json,
        nomination.array_ordinal as source_ordinal,
        committees.committee_id
    from nomination_responses
    join {{ ref('stg_congress__committee_detail') }} as committees
      on nomination_responses.parent_raw_id = committees.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(nomination_responses.response -> 'nominations', '[]'::jsonb)
    ) with ordinality as nomination(value, array_ordinal)
)
select
    source_raw_id,
    source_raw_response_version_id,
    source_crawl_run_id,
    committee_detail_raw_id,
    committee_detail_endpoint_url,
    source_endpoint_url,
    source_fetched_at,
    source_page_offset,
    source_is_complete_generation,
    source_generation_crawl_run_id,
    source_generation_page_count,
    source_generation_expected_count,
    source_generation_finished_at,
    source_ordinal,
    committee_id,
    'pn'
    || {{ json_text('nomination_json', 'number') }}
    || '-'
    || coalesce({{ json_text('nomination_json', 'partNumber') }}, '00')
    || '-'
    || {{ json_int('nomination_json', 'congress') }}::text as nomination_id,
    {{ json_int('nomination_json', 'number') }} as nomination_number,
    coalesce({{ json_text('nomination_json', 'partNumber') }}, '00') as nomination_part,
    {{ json_int('nomination_json', 'congress') }} as congress
from unnested
