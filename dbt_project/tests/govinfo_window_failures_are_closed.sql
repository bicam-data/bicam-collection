select window_id, category, window_start, window_end, status, started_at, error_message
from {{ ref('fct_govinfo_collection_windows') }}
where status = 'failed'
   or (status = 'running' and started_at < now() - interval '70 minutes')
