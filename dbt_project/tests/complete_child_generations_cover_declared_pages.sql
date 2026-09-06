/*
   A resolver row marked as a complete generation must account for every
   source-array item and every page promised by the API.  This is deliberately
   checked from the materialized observation relations, rather than inferred
   from public natural-key counts (which may legitimately deduplicate rows).
*/
{{ config(tags=["congress", "operations", "quality"]) }}

with observations as (
    select
        'bill_cosponsor'::text as observation_type,
        source_raw_id,
        source_generation_crawl_run_id,
        source_generation_page_count,
        source_generation_expected_count,
        regexp_replace(
            regexp_replace(source_endpoint_url, '([?&])(offset|limit)=[^&]*&?', '\1', 'g'),
            '[?&]$',
            ''
        ) as logical_collection
    from {{ ref('int_congress_bill_cosponsor_observations') }}
    where source_is_complete_generation

    union all

    select
        'nominee'::text as observation_type,
        source_raw_id,
        source_generation_crawl_run_id,
        source_generation_page_count,
        source_generation_expected_count,
        regexp_replace(
            regexp_replace(source_endpoint_url, '([?&])(offset|limit)=[^&]*&?', '\1', 'g'),
            '[?&]$',
            ''
        ) as logical_collection
    from {{ ref('int_congress_nominee_observations') }}
    where source_is_complete_generation
), page_counts as (
    select
        observation_type,
        logical_collection,
        source_generation_crawl_run_id,
        max(source_generation_page_count) as declared_page_count,
        max(source_generation_expected_count) as declared_item_count,
        count(distinct source_raw_id) as observed_page_count,
        count(*) as observed_item_count
    from observations
    group by observation_type, logical_collection, source_generation_crawl_run_id
)
select *
from page_counts
where observed_page_count <> declared_page_count
   or observed_item_count <> declared_item_count
