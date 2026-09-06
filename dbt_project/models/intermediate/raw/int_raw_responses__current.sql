{{ config(
    materialized="incremental",
    unique_key="raw_id",
    incremental_strategy="delete+insert",
    on_schema_change="sync_all_columns",
    indexes=[{"columns": ["raw_id"], "unique": true}, {"columns": ["source", "category", "node_type"]}]
) }}

select
    id as raw_id,
    endpoint_url,
    source,
    category,
    node_type,
    parent_id,
    response,
    response_hash,
    fetched_at,
    latest_crawl_run_id,
    raw_response_version_id
from {{ source('raw', 'raw_responses') }} as current_response
{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as existing
        where existing.raw_id = current_response.id
          and existing.response_hash = current_response.response_hash
          and existing.raw_response_version_id is not distinct from current_response.raw_response_version_id
          and existing.parent_id is not distinct from current_response.parent_id
          and existing.fetched_at is not distinct from current_response.fetched_at
          and existing.latest_crawl_run_id is not distinct from current_response.latest_crawl_run_id
    )
{% endif %}
