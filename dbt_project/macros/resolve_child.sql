{% macro resolve_child(parent_node_type, child_node_type, category, source_name="raw") -%}
/*
   Keep this macro as the source-compatible child expansion interface while
   making the expensive selection durable in
   int_congress_child_response_selections. That relation proves the newest
   complete baseline, accumulates applicable non-empty deltas, preserves
   incomplete fallback pages, and retains page/version/generation evidence.
   Downstream models still own entity-specific natural-key reconciliation.
*/
select
    child.raw_id,
    child.raw_response_version_id,
    child.latest_crawl_run_id,
    child.parent_id,
    parent.raw_id as parent_raw_id,
    parent.endpoint_url as parent_endpoint_url,
    child.endpoint_url,
    child.logical_endpoint,
    child.logical_collection,
    child.is_incremental,
    child.has_array_field,
    child.has_array_items,
    child.page_offset,
    child.page_limit,
    child.array_field_count,
    child.array_item_count,
    child.is_complete_generation,
    child.generation_crawl_run_id,
    child.generation_page_count,
    child.generation_expected_count,
    child.generation_finished_at,
    child.child_fetched_at,
    child.response
from {{ ref('int_congress_child_response_selections') }} as child
join {{ ref('int_raw_responses__current') }} as parent
  on child.parent_id = parent.raw_id
where child.category = '{{ category }}'
  and child.node_type = '{{ child_node_type }}'
  and parent.source = 'congress'
  and parent.category = '{{ category }}'
  and parent.node_type = '{{ parent_node_type }}'
{%- endmacro %}
