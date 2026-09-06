with completed as (
    select
        category,
        window_start,
        window_end,
        lag(window_end) over (
            partition by category
            order by window_start, window_end
        ) as previous_window_end
    from {{ ref('fct_govinfo_collection_windows') }}
    where status = 'completed'
)

select category, window_start, window_end, previous_window_end
from completed
where previous_window_end is not null
  and previous_window_end <> window_start
