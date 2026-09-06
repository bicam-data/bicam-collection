{{ config(
    materialized="incremental",
    unique_key="raw_response_version_id",
    incremental_strategy="delete+insert",
    on_schema_change="sync_all_columns",
    tags=["congress", "child_state"],
    indexes=[
        {"columns": ["raw_response_version_id"], "unique": true},
        {"columns": ["raw_id"]},
        {"columns": ["category", "node_type", "logical_collection"]},
        {"columns": ["logical_endpoint", "fetched_at"]}
    ]
) }}

/*
   Compute the transport-level facts used by every Congress.gov child
   resolver once. The resolver still chooses baselines and deltas; this
   relation only centralizes the expensive JSON shape walk and page facts.

   Read immutable response versions rather than only the current URL row.
   Current rows are retained for their fetch timestamp when the version is
   still current; historical versions use their last-seen/first-seen time.
   This lets a changed URL remain reproducible after a later upsert.
*/
with recursive child_responses as materialized (
    select
        versions.raw_response_id as raw_id,
        versions.raw_response_version_id,
        versions.endpoint_url,
        versions.source,
        versions.category,
        versions.node_type,
        coalesce(versions.parent_id, current_response.parent_id) as parent_id,
        versions.response,
        versions.response_hash,
        coalesce(current_response.fetched_at, versions.last_seen_at, versions.first_seen_at)
            as fetched_at,
        coalesce(current_response.latest_crawl_run_id, versions.latest_crawl_run_id)
            as latest_crawl_run_id
    from {{ ref('int_raw_response_versions') }} as versions
    left join {{ ref('int_raw_responses__current') }} as current_response
      on current_response.raw_id = versions.raw_response_id
     and current_response.raw_response_version_id = versions.raw_response_version_id
    where versions.source = 'congress'
      /* Keep the shared scan bounded to node types that are actually
         resolved by child models. Root/detail payloads can contain large
         nested arrays and do not need transport-level child selection. */
      and versions.node_type in (
          'amendment_actions',
          'amendment_amendments_to_amendment',
          'amendment_cosponsors',
          'amendment_texts',
          'bill_actions',
          'bill_amendments',
          'bill_committees',
          'bill_cosponsors',
          'bill_relatedbills',
          'bill_subjects',
          'bill_summaries',
          'bill_titles',
          'bill_text',
          'committee_bills',
          'committee_communications',
          'committee_nominations',
          'committee_reports',
          'committee_print_texts',
          'committee_report_texts',
          'member_cosponsored_legislation',
          'member_sponsored_legislation',
          'nomination_actions',
          'nomination_committees',
          'nomination_hearings',
          'nomination_nominee_detail',
          'treaty_actions',
          'treaty_committees',
          'house_requirement_matching_communications',
          'house_vote_members'
      )
      {% if is_incremental() %}
      /* Immutable versions are append-only, but a repeated fetch updates the
         version's last-seen/crawl evidence and a repair can correct parent_id.
         Revisit exactly those changed facts without rescanning every payload
         on each propagation run. */
      and not exists (
          select 1
          from {{ this }} as existing
          where existing.raw_response_version_id = versions.raw_response_version_id
            and existing.response_hash = versions.response_hash
            and existing.parent_id is not distinct from versions.parent_id
            and existing.fetched_at is not distinct from coalesce(
                current_response.fetched_at,
                versions.last_seen_at,
                versions.first_seen_at
            )
            and existing.latest_crawl_run_id is not distinct from coalesce(
                current_response.latest_crawl_run_id,
                versions.latest_crawl_run_id
            )
      )
      {% endif %}
), top_array_stats as materialized (
    select
        responses.raw_response_version_id,
        count(*) filter (
            where jsonb_typeof(response_field.field_value) = 'array'
        )::bigint as top_array_field_count,
        coalesce(sum(
            case
                when jsonb_typeof(response_field.field_value) = 'array'
                    then jsonb_array_length(response_field.field_value)
                else 0
            end
        ), 0)::bigint as top_array_item_count
    from child_responses as responses
    cross join lateral jsonb_each(
        case
            when jsonb_typeof(responses.response) = 'object' then responses.response
            else '{}'::jsonb
        end
    ) as response_field(field_name, field_value)
    group by responses.raw_response_version_id
), child_json_nodes as materialized (
    select
        responses.raw_response_version_id,
        responses.response as value,
        0 as depth
    from child_responses as responses
    join top_array_stats
      on top_array_stats.raw_response_version_id = responses.raw_response_version_id
    where top_array_stats.top_array_field_count = 0

    union all

    select
        nodes.raw_response_version_id,
        nested.value,
        nodes.depth + 1
    from child_json_nodes as nodes
    cross join lateral (
        select object_field.field_value as value
        from jsonb_each(
            case
                when jsonb_typeof(nodes.value) = 'object' then nodes.value
                else '{}'::jsonb
            end
        ) as object_field(field_name, field_value)
    ) as nested
    where jsonb_typeof(nodes.value) = 'object'
      and nodes.depth < 8
), nested_array_stats as materialized (
    select
        nodes.raw_response_version_id,
        count(*) filter (
            where nodes.depth > 0 and jsonb_typeof(nodes.value) = 'array'
        )::bigint as nested_array_field_count,
        coalesce(sum(
            case
                when nodes.depth > 1 and jsonb_typeof(nodes.value) = 'array'
                    then jsonb_array_length(nodes.value)
                else 0
            end
        ), 0)::bigint as nested_array_item_count
    from child_json_nodes as nodes
    group by nodes.raw_response_version_id
), array_stats as materialized (
    select
        top_array_stats.raw_response_version_id,
        top_array_stats.top_array_field_count,
        top_array_stats.top_array_item_count,
        coalesce(nested_array_stats.nested_array_field_count, 0)::bigint
            as nested_array_field_count,
        coalesce(nested_array_stats.nested_array_item_count, 0)::bigint
            as nested_array_item_count
    from top_array_stats
    left join nested_array_stats using (raw_response_version_id)
), annotated as (
    select
        child.*,
        /* Normalize transport parameters without discarding unknown
           parameters that may carry a meaningful source identity. */
        {{ congress_child_logical_endpoint('child.endpoint_url') }} as logical_endpoint,
        child.endpoint_url ~ '[?&](fromDateTime|toDateTime)=' as is_incremental,
        coalesce(
            nullif(array_stats.top_array_field_count, 0),
            array_stats.nested_array_field_count,
            0
        ) > 0 as has_array_field,
        coalesce(
            nullif(array_stats.top_array_item_count, 0),
            array_stats.nested_array_item_count,
            0
        ) > 0 as has_array_items,
        coalesce(
            ((regexp_match(child.endpoint_url, '[?&]offset=([0-9]+)'))[1])::bigint,
            0
        ) as page_offset,
        nullif(
            ((regexp_match(child.endpoint_url, '[?&]limit=([0-9]+)'))[1])::bigint,
            0
        ) as page_limit,
        case
            when child.response #>> '{pagination,count}' ~ '^[0-9]+$'
                then (child.response #>> '{pagination,count}')::bigint
            else null
        end as pagination_count,
        case
            when array_stats.top_array_field_count > 0
                then array_stats.top_array_field_count
            else array_stats.nested_array_field_count
        end::bigint as array_field_count,
        case
            when array_stats.top_array_field_count > 0
                then array_stats.top_array_item_count
            else array_stats.nested_array_item_count
        end::bigint as array_item_count
    from child_responses as child
    join array_stats using (raw_response_version_id)
)
select
    annotated.*,
    annotated.logical_endpoint as logical_collection,
    not (annotated.response ? 'pagination')
      and annotated.page_offset = 0
      and annotated.has_array_field as is_non_paginated_full
from annotated
