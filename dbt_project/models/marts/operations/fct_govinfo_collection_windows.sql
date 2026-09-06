{{ config(materialized='view', tags=['operations', 'govinfo', 'coverage']) }}

select
    id as window_id,
    category,
    window_start,
    window_end,
    status,
    crawl_run_id,
    list_pages,
    fetch_events,
    failed_events,
    started_at,
    finished_at,
    error_message,
    is_bootstrap,
    max(window_end) filter (where status = 'completed')
        over (partition by category) as completed_through
from {{ source('raw', 'govinfo_collection_windows') }}
