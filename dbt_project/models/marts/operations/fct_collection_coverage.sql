{{ config(tags=["operations", "coverage"]) }}

with rules as (
    select * from {{ ref('collection_coverage_rules') }}
),
responses as (
    select
        source,
        category,
        node_type,
        coalesce((regexp_match(endpoint_url, '/([0-9]{2,3})(?:/|\\?)'))[1], 'unpartitioned') as partition_value,
        count(*) as current_response_count,
        min(fetched_at) as first_fetched_at,
        max(fetched_at) as last_fetched_at,
        (array_agg(latest_crawl_run_id order by fetched_at desc nulls last)
            filter (where latest_crawl_run_id is not null))[1] as latest_crawl_run_id
    from {{ source('raw', 'raw_responses') }}
    group by 1, 2, 3, 4
),
runs as (
    select
        category,
        max(finished_at) filter (where status = 'completed') as last_completed_at,
        count(*) filter (where status = 'failed') as failed_run_count,
        count(*) filter (where status = 'running') as running_run_count
    from {{ source('raw', 'crawl_runs') }}
    group by 1
),
joined as (
    select
        coalesce(responses.source, rules.source) as source,
        coalesce(responses.category, rules.category) as category,
        coalesce(responses.node_type, rules.node_type) as node_type,
        coalesce(responses.partition_value, 'unpartitioned') as partition_value,
        coalesce(responses.current_response_count, 0) as current_response_count,
        responses.first_fetched_at,
        responses.last_fetched_at,
        responses.latest_crawl_run_id,
        runs.last_completed_at,
        coalesce(runs.failed_run_count, 0) as failed_run_count,
        coalesce(runs.running_run_count, 0) as running_run_count,
        coalesce(rules.expected_scope, 'unregistered') as expected_scope,
        coalesce(rules.authoritative_source, responses.source) as authoritative_source,
        coalesce(rules.bulk_bootstrap, 'false') as bulk_bootstrap,
        coalesce(rules.scope_notes, 'Observed response group is not covered by a declared rule') as scope_notes
    from responses
    full outer join rules
      on rules.source = responses.source
     and rules.category = responses.category
     and rules.node_type in ('*', responses.node_type)
    left join runs
      on coalesce(responses.category, rules.category) = runs.category
)

select
    {{ hash_to_uuid("joined.source || '|' || joined.category || '|' || joined.node_type || '|' || joined.partition_value") }} as coverage_id,
    source,
    category,
    node_type,
    partition_value,
    current_response_count,
    current_response_count > 0 as has_observed_data,
    expected_scope,
    authoritative_source,
    bulk_bootstrap,
    scope_notes,
    first_fetched_at,
    last_fetched_at,
    latest_crawl_run_id,
    last_completed_at,
    failed_run_count,
    running_run_count
from joined
