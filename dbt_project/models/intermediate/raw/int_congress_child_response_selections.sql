{{ config(
    materialized="table",
    tags=["congress", "child_state"],
    indexes=[
        {"columns": ["raw_response_version_id"], "unique": true},
        {"columns": ["raw_id"]},
        {"columns": ["category", "node_type", "logical_collection"]},
        {"columns": ["category", "node_type", "parent_id"]},
        {"columns": ["logical_endpoint", "child_fetched_at"]},
        {"columns": [
            "category", "node_type", "logical_collection", "latest_crawl_run_id",
            "is_incremental", "page_offset", "raw_id"
        ]},
        {"columns": ["category", "node_type", "logical_endpoint", "child_fetched_at", "raw_id"]}
    ]
) }}

/*
   Materialize the selected response pages once for every registered
   Congress.gov child collection. This is the durable Package-B interface:
   every selected row carries endpoint, logical collection, page, fetch/version
   lineage, full/delta classification, and completeness evidence. The
   resolver macro is intentionally only a source-compatible filtered view of
   this relation; source-gap reporting can therefore see incomplete empty
   responses that produce no unnested child rows.
*/
with generation_pages as materialized (
    select
        annotated.*,
        case
            when annotated.pagination_count is not null
                then annotated.pagination_count
            when not (annotated.response ? 'pagination')
             and annotated.page_offset = 0
             and annotated.array_field_count > 0
                then annotated.array_item_count
            else null
        end as expected_generation_count,
        annotated.page_offset + annotated.array_item_count as page_end_offset,
        lag(annotated.page_offset + annotated.array_item_count) over (
            partition by annotated.category,
                         annotated.node_type,
                         annotated.logical_collection,
                         annotated.latest_crawl_run_id,
                         annotated.is_incremental
            order by annotated.page_offset, annotated.raw_id
        ) as previous_page_end_offset
    from {{ ref('int_congress_child_response_selection') }} as annotated
    where annotated.latest_crawl_run_id is not null
      and annotated.has_array_field
), generation_summaries as materialized (
    select
        generation_pages.category,
        generation_pages.node_type,
        generation_pages.logical_collection,
        generation_pages.latest_crawl_run_id as generation_crawl_run_id,
        generation_pages.is_incremental,
        min(generation_pages.page_offset) as minimum_page_offset,
        max(generation_pages.page_end_offset) as maximum_page_end_offset,
        min(generation_pages.expected_generation_count) as minimum_expected_count,
        max(generation_pages.expected_generation_count) as maximum_expected_count,
        sum(generation_pages.array_item_count)::bigint as observed_generation_count,
        count(*)::bigint as generation_page_count,
        max(generation_pages.fetched_at) as generation_finished_at,
        bool_and(
            generation_pages.previous_page_end_offset is null
            or generation_pages.previous_page_end_offset = generation_pages.page_offset
        ) as pages_are_contiguous
    from generation_pages
    where generation_pages.expected_generation_count is not null
    group by
        generation_pages.category,
        generation_pages.node_type,
        generation_pages.logical_collection,
        generation_pages.latest_crawl_run_id,
        generation_pages.is_incremental
), complete_generations as materialized (
    select summaries.*
    from generation_summaries as summaries
    where summaries.minimum_page_offset = 0
      and summaries.minimum_expected_count = summaries.maximum_expected_count
      and summaries.observed_generation_count = summaries.maximum_expected_count
      and summaries.maximum_page_end_offset = summaries.maximum_expected_count
      and summaries.pages_are_contiguous
), latest_complete_full_generations as materialized (
    select distinct on (
        complete.category,
        complete.node_type,
        complete.logical_collection
    )
        complete.category,
        complete.node_type,
        complete.logical_collection,
        complete.generation_crawl_run_id,
        complete.generation_page_count,
        complete.maximum_expected_count as generation_expected_count,
        complete.generation_finished_at
    from complete_generations as complete
    where not complete.is_incremental
    order by
        complete.category,
        complete.node_type,
        complete.logical_collection,
        complete.generation_finished_at desc,
        complete.generation_crawl_run_id desc
), complete_children as materialized (
    select
        annotated.*,
        row_number() over (
            partition by annotated.category,
                         annotated.node_type,
                         annotated.logical_endpoint
            order by annotated.fetched_at desc, annotated.raw_id desc
        ) as complete_rank
    from {{ ref('int_congress_child_response_selection') }} as annotated
    where not annotated.is_incremental
      and annotated.has_array_field
), selected_children as (
    select
        annotated.*,
        (
            latest_full.generation_crawl_run_id is not null
            and annotated.latest_crawl_run_id = latest_full.generation_crawl_run_id
            and not annotated.is_incremental
        ) or annotated.is_non_paginated_full as is_complete_generation,
        case
            when latest_full.generation_crawl_run_id is not null
                then latest_full.generation_crawl_run_id
            when annotated.is_non_paginated_full
                then annotated.latest_crawl_run_id
            else null
        end as generation_crawl_run_id,
        case
            when latest_full.generation_crawl_run_id is not null
                then latest_full.generation_page_count
            when annotated.is_non_paginated_full then 1::bigint
            else null
        end as generation_page_count,
        case
            when latest_full.generation_crawl_run_id is not null
                then latest_full.generation_expected_count
            when annotated.is_non_paginated_full then annotated.array_item_count
            else null
        end as generation_expected_count,
        case
            when latest_full.generation_crawl_run_id is not null
                then latest_full.generation_finished_at
            when annotated.is_non_paginated_full then annotated.fetched_at
            else null
        end as generation_finished_at
    from {{ ref('int_congress_child_response_selection') }} as annotated
    left join complete_children as complete
      on complete.category = annotated.category
     and complete.node_type = annotated.node_type
     and complete.logical_endpoint = annotated.logical_endpoint
     and complete.complete_rank = 1
    left join latest_complete_full_generations as latest_full
      on latest_full.category = annotated.category
     and latest_full.node_type = annotated.node_type
     and latest_full.logical_collection = annotated.logical_collection
    where (
        not annotated.is_incremental
        and (
            (
                latest_full.generation_crawl_run_id is not null
                and annotated.latest_crawl_run_id = latest_full.generation_crawl_run_id
            )
            or (
                latest_full.generation_crawl_run_id is null
                and complete.raw_response_version_id = annotated.raw_response_version_id
            )
        )
    )
    or (
        annotated.is_incremental
        and annotated.has_array_items
        and (
            (
                latest_full.generation_crawl_run_id is not null
                and annotated.fetched_at >= latest_full.generation_finished_at
            )
            or (
                latest_full.generation_crawl_run_id is null
                and (
                    complete.raw_id is null
                    or annotated.fetched_at >= complete.fetched_at
                )
            )
        )
    )
)
select
    selected.raw_id,
    selected.raw_response_version_id,
    selected.endpoint_url,
    selected.source,
    selected.category,
    selected.node_type,
    selected.parent_id,
    selected.response,
    selected.response_hash,
    selected.fetched_at as child_fetched_at,
    selected.latest_crawl_run_id,
    selected.logical_endpoint,
    selected.logical_collection,
    selected.is_incremental,
    selected.has_array_field,
    selected.has_array_items,
    selected.page_offset,
    selected.page_limit,
    selected.array_field_count,
    selected.array_item_count,
    selected.is_complete_generation,
    selected.generation_crawl_run_id,
    selected.generation_page_count,
    selected.generation_expected_count,
    selected.generation_finished_at
from selected_children as selected
