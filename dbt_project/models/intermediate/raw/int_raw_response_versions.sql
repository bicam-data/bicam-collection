{{ config(
    materialized="incremental",
    unique_key="raw_response_version_id",
    incremental_strategy="delete+insert",
    on_schema_change="sync_all_columns",
    indexes=[{"columns": ["raw_response_version_id"], "unique": true}, {"columns": ["raw_response_id"]}]
) }}

select
    id as raw_response_version_id,
    raw_response_id,
    endpoint_url,
    source,
    category,
    node_type,
    parent_id,
    response,
    response_hash,
    first_seen_at,
    last_seen_at,
    first_crawl_run_id,
    latest_crawl_run_id
from {{ source('raw', 'raw_response_versions') }} as versions
{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as existing
        where existing.raw_response_version_id = versions.id
          and existing.response_hash = versions.response_hash
          and existing.last_seen_at is not distinct from versions.last_seen_at
          and existing.parent_id is not distinct from versions.parent_id
    )
{% endif %}
