{{ config(
    materialized="table",
    indexes=[
        {"columns": ["format_key"], "unique": true},
        {"columns": ["amendment_id"]}
    ],
    tags=["amendments", "raw", "child_state"]
) }}

select
    versions.text_version_key,
    versions.text_raw_id,
    versions.amendment_detail_raw_id,
    versions.text_endpoint_url,
    versions.raw_response_version_id,
    versions.source_crawl_run_id,
    versions.source_fetched_at,
    versions.source_page_offset,
    versions.source_is_complete_generation,
    versions.source_generation_crawl_run_id,
    versions.source_generation_page_count,
    versions.source_generation_expected_count,
    versions.source_generation_finished_at,
    versions.amendment_id,
    versions.text_version_ordinal,
    format_item.ordinality::integer as format_ordinal,
    versions.text_version_key || ':' || format_item.ordinality::text as format_key,
    versions.text_version_json,
    format_item.value as format_json
from {{ ref('int_congress_amendment_text_versions') }} as versions
cross join lateral jsonb_array_elements(
    case
        when jsonb_typeof(versions.text_version_json -> 'formats') = 'array'
            then versions.text_version_json -> 'formats'
        else '[]'::jsonb
    end
) with ordinality as format_item(value, ordinality)
