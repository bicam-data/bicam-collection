{{ config(tags=["operations", "coverage"]) }}

with rules as (
    select * from {{ ref('collection_coverage_rules') }}
),
scope_partitions as (
    select
        source,
        category,
        partition_field,
        regexp_replace(trim(partition_value), '^p', '') as partition_value,
        completeness
    from {{ ref('collection_scope_partitions') }}
    cross join lateral regexp_split_to_table(partition_values, ',') as partition_value
),
responses as (
    select
        source,
        category,
        node_type,
        {{ collection_partition_value('endpoint_url', 'category') }} as partition_value,
        count(*) as current_response_count,
        min(fetched_at) as first_fetched_at,
        max(fetched_at) as last_fetched_at,
        (array_agg(latest_crawl_run_id order by fetched_at desc nulls last)
            filter (where latest_crawl_run_id is not null))[1] as latest_crawl_run_id
    from {{ source('raw', 'raw_responses') }}
    group by 1, 2, 3, 4
),
response_summaries as (
    select
        source,
        category,
        partition_value,
        sum(current_response_count) as current_response_count,
        min(first_fetched_at) as first_fetched_at,
        max(last_fetched_at) as last_fetched_at,
        (array_agg(latest_crawl_run_id order by last_fetched_at desc nulls last)
            filter (where latest_crawl_run_id is not null))[1] as latest_crawl_run_id
    from responses
    group by 1, 2, 3
),
run_status as (
    select
        category,
        max(finished_at) filter (where status = 'completed') as last_completed_at,
        count(*) filter (where status = 'failed') as failed_run_count,
        count(*) filter (where status = 'running') as running_run_count
    from {{ source('raw', 'crawl_runs') }}
    group by 1
),
run_partition_paths as (
    /*
       Most runs have expected-fetch rows, so one indexed representative URL
       per run is enough to identify the historical Congress partition. Add
       current raw-response lineage and failures to preserve partitions for
       global incremental runs, whose child URLs can span many Congresses.
       The previous implementation scanned all 157M fetch events before
       filtering to the latest run, which made this operational model spill
       for more than ten minutes.
    */
    select
        runs.id as crawl_run_id,
        runs.category,
        runs.mode,
        runs.status,
        runs.finished_at,
        runs.started_at,
        expected.endpoint_url
    from {{ source('raw', 'crawl_runs') }} as runs
    join lateral (
        select expected.endpoint_url
        from {{ source('raw', 'crawl_run_expected_fetches') }} as expected
        where expected.crawl_run_id = runs.id
        order by expected.expected_at desc, expected.id desc
        limit 1
    ) as expected on true

    union all

    select
        runs.id as crawl_run_id,
        runs.category,
        runs.mode,
        runs.status,
        runs.finished_at,
        runs.started_at,
        responses.endpoint_url
    from {{ source('raw', 'crawl_runs') }} as runs
    join {{ source('raw', 'raw_responses') }} as responses
      on responses.latest_crawl_run_id = runs.id

    union all

    select
        runs.id as crawl_run_id,
        runs.category,
        runs.mode,
        runs.status,
        runs.finished_at,
        runs.started_at,
        failed.endpoint_url
    from {{ source('raw', 'crawl_runs') }} as runs
    join {{ source('raw', 'failed_requests') }} as failed
      on failed.crawl_run_id = runs.id

    union all

    /* A few legacy/targeted runs predate expected-fetch recording. */
    select
        runs.id as crawl_run_id,
        runs.category,
        runs.mode,
        runs.status,
        runs.finished_at,
        runs.started_at,
        fetches.endpoint_url
    from {{ source('raw', 'crawl_runs') }} as runs
    join lateral (
        select fetches.endpoint_url
        from {{ source('raw', 'crawl_run_fetches') }} as fetches
        where fetches.crawl_run_id = runs.id
        order by fetches.fetched_at desc, fetches.id desc
        limit 1
    ) as fetches on true
    where not exists (
        select 1
        from {{ source('raw', 'crawl_run_expected_fetches') }} as expected
        where expected.crawl_run_id = runs.id
    )
),
run_partitions as (
    select distinct
        crawl_run_id,
        category,
        mode,
        status,
        finished_at,
        started_at,
        {{ collection_partition_value('endpoint_url', 'category') }} as partition_value
    from run_partition_paths
),
latest_runs as (
    /*
       Coverage event counts intentionally describe the latest completed,
       non-retry run for each category/partition.  ``current_response_count``
       remains the cumulative current-state archive count; full-scope root and
       child completeness is enforced by the dedicated coverage ledgers.
    */
    select distinct on (category, partition_value)
        crawl_run_id,
        category,
        mode,
        partition_value
    from run_partitions
    where status = 'completed'
      and mode not like 'retry_%'
    order by category, partition_value, finished_at desc nulls last, started_at desc
),
activity_events as (
    select
        fetches.crawl_run_id,
        runs.category,
        fetches.node_type,
        fetches.endpoint_url,
        fetches.parent_endpoint_url,
        {{ collection_partition_value('fetches.endpoint_url', 'runs.category') }} as partition_value,
        fetches.outcome
    from {{ source('raw', 'crawl_run_fetches') }} as fetches
    join {{ source('raw', 'crawl_runs') }} as runs on runs.id = fetches.crawl_run_id
    join latest_runs
      on latest_runs.crawl_run_id = fetches.crawl_run_id
     and latest_runs.category = runs.category
     and latest_runs.partition_value = {{ collection_partition_value('fetches.endpoint_url', 'runs.category') }}

    union all

    select
        failed.crawl_run_id,
        failed.category,
        failed.node_type,
        failed.endpoint_url,
        failed.parent_endpoint_url,
        {{ collection_partition_value('failed.endpoint_url', 'failed.category') }} as partition_value,
        'failed' as outcome
    from {{ source('raw', 'failed_requests') }} as failed
    join latest_runs
      on latest_runs.crawl_run_id = failed.crawl_run_id
     and latest_runs.category = failed.category
     and latest_runs.partition_value = {{ collection_partition_value('failed.endpoint_url', 'failed.category') }}
),
latest_activity as (
    select distinct
        events.category,
        events.node_type,
        events.partition_value,
        events.endpoint_url,
        events.parent_endpoint_url,
        events.outcome
    from activity_events as events
),
activity_counts as (
    select
        category,
        node_type,
        partition_value,
        count(distinct endpoint_url) as scheduled_fetch_count,
        count(distinct endpoint_url) filter (
            where outcome in ('fetched', 'skipped_existing', 'skipped_existing_backfill')
        ) as successful_fetch_count,
        count(distinct endpoint_url) filter (where outcome = 'failed') as failed_fetch_count,
        count(distinct endpoint_url) filter (where parent_endpoint_url is not null) as child_scheduled_fetch_count,
        count(distinct endpoint_url) filter (
            where parent_endpoint_url is not null
              and outcome in ('fetched', 'skipped_existing', 'skipped_existing_backfill')
        ) as child_successful_fetch_count,
        count(distinct endpoint_url) filter (
            where parent_endpoint_url is not null and outcome = 'failed'
        ) as child_failed_fetch_count
    from latest_activity
    group by 1, 2, 3
),
expected_events as (
    select
        expected.crawl_run_id,
        runs.category,
        expected.node_type,
        expected.endpoint_url,
        expected.parent_endpoint_url,
        {{ collection_partition_value('expected.endpoint_url', 'runs.category') }} as partition_value
    from {{ source('raw', 'crawl_run_expected_fetches') }} as expected
    join {{ source('raw', 'crawl_runs') }} as runs on runs.id = expected.crawl_run_id
),
latest_expected as (
    select distinct
        events.category,
        events.node_type,
        events.partition_value,
        events.endpoint_url,
        events.parent_endpoint_url
    from expected_events as events
    join latest_runs
      on latest_runs.crawl_run_id = events.crawl_run_id
     and latest_runs.category = events.category
     and latest_runs.partition_value = events.partition_value
),
expected_counts as (
    select
        category,
        node_type,
        partition_value,
        count(distinct endpoint_url) as expected_fetch_count,
        count(distinct endpoint_url) filter (where parent_endpoint_url is not null) as expected_child_fetch_count
    from latest_expected
    group by 1, 2, 3
),
node_rows as (
    select
        responses.source,
        responses.category,
        responses.node_type,
        responses.partition_value,
        responses.current_response_count,
        true as has_observed_data,
        coalesce(rules.expected_scope, 'unregistered') as expected_scope,
        coalesce(rules.authoritative_source, responses.source) as authoritative_source,
        coalesce(rules.bulk_bootstrap, 'false') as bulk_bootstrap,
        coalesce(rules.scope_notes, 'Observed response group is not covered by a declared rule') as scope_notes,
        responses.first_fetched_at,
        responses.last_fetched_at,
        authoritative_run.crawl_run_id as latest_crawl_run_id,
        run_status.last_completed_at,
        coalesce(run_status.failed_run_count, 0) as failed_run_count,
        coalesce(run_status.running_run_count, 0) as running_run_count,
        coalesce(activity.scheduled_fetch_count, 0) as scheduled_fetch_count,
        coalesce(activity.successful_fetch_count, 0) as successful_fetch_count,
        coalesce(activity.failed_fetch_count, 0) as failed_fetch_count,
        coalesce(expected.expected_fetch_count, 0) as expected_fetch_count,
        coalesce(expected.expected_child_fetch_count, 0) as expected_child_fetch_count,
        coalesce(activity.child_scheduled_fetch_count, 0) as child_scheduled_fetch_count,
        coalesce(activity.child_successful_fetch_count, 0) as child_successful_fetch_count,
        coalesce(activity.child_failed_fetch_count, 0) as child_failed_fetch_count
    from responses
    left join rules
      on rules.source = responses.source
     and rules.category = responses.category
     and rules.node_type in ('*', responses.node_type)
    left join run_status on run_status.category = responses.category
    left join latest_runs as authoritative_run
      on authoritative_run.category = responses.category
     and authoritative_run.partition_value = responses.partition_value
    left join activity_counts as activity
      on activity.category = responses.category
     and activity.node_type = responses.node_type
     and activity.partition_value = responses.partition_value
    left join expected_counts as expected
      on expected.category = responses.category
     and expected.node_type = responses.node_type
     and expected.partition_value = responses.partition_value
),
scope_rows as (
    select
        scope.source,
        scope.category,
        '*' as node_type,
        scope.partition_value,
        coalesce(summary.current_response_count, 0) as current_response_count,
        coalesce(summary.current_response_count, 0) > 0 as has_observed_data,
        coalesce(rules.expected_scope, 'full_congress_scope') as expected_scope,
        coalesce(rules.authoritative_source, scope.source) as authoritative_source,
        coalesce(rules.bulk_bootstrap, 'false') as bulk_bootstrap,
        coalesce(rules.scope_notes, 'Declared partition from the collection scope contract') as scope_notes,
        summary.first_fetched_at,
        summary.last_fetched_at,
        authoritative_run.crawl_run_id as latest_crawl_run_id,
        run_status.last_completed_at,
        coalesce(run_status.failed_run_count, 0) as failed_run_count,
        coalesce(run_status.running_run_count, 0) as running_run_count,
        coalesce(sum(activity.scheduled_fetch_count), 0) as scheduled_fetch_count,
        coalesce(sum(activity.successful_fetch_count), 0) as successful_fetch_count,
        coalesce(sum(activity.failed_fetch_count), 0) as failed_fetch_count,
        coalesce(sum(expected.expected_fetch_count), 0) as expected_fetch_count,
        coalesce(sum(expected.expected_child_fetch_count), 0) as expected_child_fetch_count,
        coalesce(sum(activity.child_scheduled_fetch_count), 0) as child_scheduled_fetch_count,
        coalesce(sum(activity.child_successful_fetch_count), 0) as child_successful_fetch_count,
        coalesce(sum(activity.child_failed_fetch_count), 0) as child_failed_fetch_count
    from scope_partitions as scope
    left join response_summaries as summary
      on summary.source = scope.source
     and summary.category = scope.category
     and summary.partition_value = scope.partition_value
    left join rules
      on rules.source = scope.source
     and rules.category = scope.category
     and rules.node_type = '*'
    left join run_status on run_status.category = scope.category
    left join latest_runs as authoritative_run
      on authoritative_run.category = scope.category
     and authoritative_run.partition_value = scope.partition_value
    left join activity_counts as activity
      on activity.category = scope.category
     and activity.partition_value = scope.partition_value
    left join expected_counts as expected
      on expected.category = scope.category
     and expected.partition_value = scope.partition_value
    group by
        scope.source, scope.category, scope.partition_value,
        summary.current_response_count, summary.first_fetched_at,
        summary.last_fetched_at, authoritative_run.crawl_run_id,
        rules.expected_scope, rules.authoritative_source, rules.bulk_bootstrap,
        rules.scope_notes, run_status.last_completed_at, run_status.failed_run_count,
        run_status.running_run_count
)

select
    {{ hash_to_uuid("joined.source || '|' || joined.category || '|' || joined.node_type || '|' || joined.partition_value") }} as coverage_id,
    joined.source,
    joined.category,
    joined.node_type,
    joined.partition_value,
    joined.current_response_count,
    joined.has_observed_data,
    joined.expected_scope,
    joined.authoritative_source,
    joined.bulk_bootstrap,
    joined.scope_notes,
    joined.first_fetched_at,
    joined.last_fetched_at,
    joined.latest_crawl_run_id,
    joined.last_completed_at,
    joined.failed_run_count,
    joined.running_run_count,
    joined.scheduled_fetch_count,
    joined.successful_fetch_count,
    joined.failed_fetch_count,
    joined.expected_fetch_count,
    joined.expected_child_fetch_count,
    joined.child_scheduled_fetch_count,
    joined.child_successful_fetch_count,
    joined.child_failed_fetch_count,
    greatest(joined.expected_fetch_count - joined.successful_fetch_count - joined.failed_fetch_count, 0) as unsatisfied_fetch_count,
    greatest(joined.expected_child_fetch_count - joined.child_successful_fetch_count - joined.child_failed_fetch_count, 0) as child_unsatisfied_fetch_count
from (
    select * from node_rows
    union all
    select * from scope_rows
) as joined
