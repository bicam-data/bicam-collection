{{ config(tags=["operations", "coverage"]) }}

with responses as (
    select
        source,
        category,
        node_type,
        count(*) as current_response_count,
        min(fetched_at) as first_fetched_at,
        max(fetched_at) as last_fetched_at,
        (array_agg(latest_crawl_run_id order by fetched_at desc nulls last)
            filter (where latest_crawl_run_id is not null))[1] as latest_crawl_run_id
    from {{ source('raw', 'raw_responses') }}
    group by 1, 2, 3
),
runs as (
    select
        category,
        max(finished_at) filter (where status = 'completed') as last_completed_at,
        count(*) filter (where status = 'failed') as failed_run_count,
        count(*) filter (where status = 'running') as running_run_count
    from {{ source('raw', 'crawl_runs') }}
    group by 1
)

select
    {{ hash_to_uuid("responses.source || '|' || responses.category || '|' || responses.node_type") }} as coverage_id,
    responses.source,
    responses.category,
    responses.node_type,
    responses.current_response_count,
    responses.first_fetched_at,
    responses.last_fetched_at,
    responses.latest_crawl_run_id,
    runs.last_completed_at,
    coalesce(runs.failed_run_count, 0) as failed_run_count,
    coalesce(runs.running_run_count, 0) as running_run_count
from responses
left join runs
  on responses.category = runs.category

