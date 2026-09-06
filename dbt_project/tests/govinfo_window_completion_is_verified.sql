select
    window_id,
    category,
    window_start,
    window_end,
    crawl_run_id,
    list_pages,
    fetch_events,
    failed_events
from {{ ref('fct_govinfo_collection_windows') }}
where status = 'completed'
  and not is_bootstrap
  and (
       crawl_run_id is null
       or coalesce(list_pages, 0) < 1
       or coalesce(fetch_events, 0) < 1
       or coalesce(failed_events, 0) <> 0
  )
