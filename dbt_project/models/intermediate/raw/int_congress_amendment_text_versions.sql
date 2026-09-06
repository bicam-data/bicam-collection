{{ config(
    materialized="table",
    indexes=[
        {"columns": ["text_version_key"], "unique": true},
        {"columns": ["amendment_id"]}
    ],
    tags=["amendments", "raw", "child_state"]
) }}

/*
   Amendment text is a child collection. Resolve the complete baseline and
   applicable non-empty deltas before expanding textVersions so an adjacent
   incremental window cannot hide an earlier rendition. This table keeps the
   source-array position and immutable response evidence; the public relation
   remains in its historical column shape and intentionally has grouping-key
   semantics rather than claiming a unique rendition key.
*/
with text_responses as (
    {{ resolve_child('amendment_detail', 'amendment_texts', 'amendments') }}
), amendment_detail as (
    select raw_id, amendment_id
    from {{ ref('stg_congress__amendment_detail') }}
), expanded as (
    select
        text_responses.raw_id as text_raw_id,
        text_responses.parent_raw_id as amendment_detail_raw_id,
        text_responses.parent_endpoint_url as amendment_detail_endpoint_url,
        text_responses.endpoint_url as text_endpoint_url,
        text_responses.raw_response_version_id,
        text_responses.latest_crawl_run_id as source_crawl_run_id,
        text_responses.child_fetched_at as source_fetched_at,
        text_responses.page_offset as source_page_offset,
        text_responses.is_complete_generation as source_is_complete_generation,
        text_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        text_responses.generation_page_count as source_generation_page_count,
        text_responses.generation_expected_count as source_generation_expected_count,
        text_responses.generation_finished_at as source_generation_finished_at,
        amendment_detail.amendment_id,
        text_version.ordinality::integer as text_version_ordinal,
        text_version.value as text_version_json
    from text_responses
    join amendment_detail
      on text_responses.parent_raw_id = amendment_detail.raw_id
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(text_responses.response -> 'textVersions') = 'array'
                then text_responses.response -> 'textVersions'
            else '[]'::jsonb
        end
    ) with ordinality as text_version(value, ordinality)
), keyed as (
    select
        expanded.*,
        text_raw_id::text || ':' || text_version_ordinal::text as text_version_key
    from expanded
)

select *
from keyed
