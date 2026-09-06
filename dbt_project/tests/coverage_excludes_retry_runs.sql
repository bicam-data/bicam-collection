{{ config(tags=["operations", "coverage"]) }}

select coverage.latest_crawl_run_id
from {{ ref('fct_collection_coverage') }} as coverage
join {{ source('raw', 'crawl_runs') }} as runs
  on runs.id = coverage.latest_crawl_run_id
where runs.mode like 'retry_%'
