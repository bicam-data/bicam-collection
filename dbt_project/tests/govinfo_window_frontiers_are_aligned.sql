with frontiers as (
    select category, max(window_end) as completed_through
    from {{ ref('fct_govinfo_collection_windows') }}
    where status = 'completed'
    group by category
)

select
    count(*) as category_count,
    min(completed_through) as minimum_frontier,
    max(completed_through) as maximum_frontier
from frontiers
having count(*) <> 4
    or min(completed_through) <> max(completed_through)
