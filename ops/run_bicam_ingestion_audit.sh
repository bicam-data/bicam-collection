#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"
bicam_load_project_env

report_dir="$reports_root"
history_dir="$report_dir/audits"
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
latest="$report_dir/ingestion-audit-latest.txt"
history="$history_dir/$timestamp.txt"
mkdir -p "$history_dir"
cd "$project_root" || exit 1

# The audit reads materialized dbt relations while ingestion lanes may be
# rebuilding them.  Serialize with the shared ingestion lock so a report
# cannot observe a relation between DROP and CREATE (the same race the
# research-snapshot runner guards against).  Waiting consumes no API quota.
lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
lock_wait_seconds=${BICAM_AUDIT_LOCK_WAIT_SECONDS:-1800}
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
  echo "ingestion audit lock wait exceeded ${lock_wait_seconds}s" >&2
  exit 0
fi

temporary=$(mktemp "$report_dir/.ingestion-audit.XXXXXX")
trap 'rm -f "$temporary"' EXIT

{
  date -u
  echo "crawl_run_status"
  bicam_psql -At -c \
    "select status, count(*) from crawl_runs group by status order by status;"
  echo "govinfo_incremental_window_frontier"
  bicam_psql -At -F '|' -c \
    "with completed as (
       select category, max(window_end) as completed_through
         from govinfo_collection_windows
        where status='completed'
        group by category
     ), latest as (
       select distinct on (category)
              category, window_start, window_end, status,
              list_pages, fetch_events, failed_events
         from govinfo_collection_windows
        order by category, window_end desc, started_at desc
     )
     select latest.category,
            completed.completed_through,
            latest.window_start,
            latest.window_end,
            latest.status,
            coalesce(latest.list_pages,0),
            coalesce(latest.fetch_events,0),
            coalesce(latest.failed_events,0)
       from latest left join completed using(category)
      order by latest.category;"
  echo "govinfo_incomplete_windows"
  bicam_psql -At -F '|' -c \
    "select category, window_start, window_end, status,
            coalesce(failed_events,0), coalesce(error_message,'')
       from govinfo_collection_windows
      where status <> 'completed'
      order by window_start, category;"
  echo "unresolved_retryable_failures"
  # Keep the recovery queue visible across usage resets.  Count distinct URLs
  # that still have no current successful payload; repeated attempts for the
  # same URL are retained in the ledger but must not inflate the queue size.
  # Older crawler versions stored query parameters in arrival order, while
  # current URLs are canonicalized lexically.  Compare an order-insensitive
  # URL key so a successful fetch is not reported as unresolved merely because
  # ``offset,limit,format`` and ``format,limit,offset`` were stored differently.
  bicam_psql -At -F '|' -c \
    "with latest_failures as (
       select distinct on (f.category, f.endpoint_url, f.node_type)
              f.category, f.http_status, f.endpoint_url, f.node_type, f.failed_at,
              exists (
                select 1
                from failed_requests prior_500
                where prior_500.category = f.category
                  and prior_500.endpoint_url = f.endpoint_url
                  and prior_500.node_type = f.node_type
                  and prior_500.http_status = 500
              ) as has_source_500
       from failed_requests f
       order by f.category, f.endpoint_url, f.node_type, f.failed_at desc
     ), failed_keys as (
       select f.category,
              f.http_status,
              f.endpoint_url,
              f.failed_at,
              case when strpos(f.endpoint_url, '?') = 0 then f.endpoint_url
                   else split_part(f.endpoint_url, '?', 1) || '?' ||
                        (select string_agg(part, '&' order by part)
                           from unnest(string_to_array(split_part(f.endpoint_url, '?', 2), '&')) as query_parts(part))
              end as endpoint_key
       from latest_failures f
       where (f.http_status = any(array[408,425,429,502,503,504,520,522,523,524,525])
              or f.http_status is null)
         and not f.has_source_500
         and f.failed_at < now() - interval '15 minutes'
         and not (
           right(f.node_type, 5) = '_list'
           and exists (
             select 1
             from fct_pagination_coverage coverage
             where coverage.category = f.category
               and coverage.node_type = f.node_type
               and coverage.missing_item_count = 0
               and coverage.enumerated_item_count >= coverage.expected_item_count
               and coverage.partition_value = coalesce(
                 (regexp_match(
                   f.endpoint_url,
                   '/(?:amendment|bill|committee-meeting|committee-print|committee-report|committee|hearing|nomination|treaty)/([0-9]{1,3})(?:/|[?]|$)'
                 ))[1],
                 case when f.category = 'members' then '*' end
               )
           )
         )
     ), response_keys as (
       select distinct
              case when strpos(r.endpoint_url, '?') = 0 then r.endpoint_url
                   else split_part(r.endpoint_url, '?', 1) || '?' ||
                        (select string_agg(part, '&' order by part)
                           from unnest(string_to_array(split_part(r.endpoint_url, '?', 2), '&')) as query_parts(part))
              end as endpoint_key
       from raw_response_current r
     )
     select f.category,
            coalesce(f.http_status::text, 'none') as http_status,
            count(distinct f.endpoint_url) as unresolved_urls,
            min(f.failed_at) as first_failure,
            max(f.failed_at) as last_failure
       from failed_keys f
       left join response_keys r on r.endpoint_key = f.endpoint_key
      where r.endpoint_key is null
      group by f.category, f.http_status
      order by f.category, f.http_status;"
  echo "known_source_500_failures"
  bicam_psql -At -F '|' -c \
    "with latest_failures as (
       select distinct on (f.category, f.endpoint_url, f.node_type)
              f.category, f.http_status, f.endpoint_url, f.node_type, f.failed_at,
              exists (
                select 1
                from failed_requests prior_500
                where prior_500.category = f.category
                  and prior_500.endpoint_url = f.endpoint_url
                  and prior_500.node_type = f.node_type
                  and prior_500.http_status = 500
              ) as has_source_500
       from failed_requests f
       order by f.category, f.endpoint_url, f.node_type, f.failed_at desc
     ), failed_keys as (
       select f.category,
              f.http_status,
              f.endpoint_url,
              f.failed_at,
              case when strpos(f.endpoint_url, '?') = 0 then f.endpoint_url
                   else split_part(f.endpoint_url, '?', 1) || '?' ||
                        (select string_agg(part, '&' order by part)
                           from unnest(string_to_array(split_part(f.endpoint_url, '?', 2), '&')) as query_parts(part))
              end as endpoint_key
       from latest_failures f
       where (f.http_status = 500 or f.has_source_500)
         and f.failed_at < now() - interval '15 minutes'
         and not (
           right(f.node_type, 5) = '_list'
           and exists (
             select 1
             from fct_pagination_coverage coverage
             where coverage.category = f.category
               and coverage.node_type = f.node_type
               and coverage.missing_item_count = 0
               and coverage.enumerated_item_count >= coverage.expected_item_count
               and coverage.partition_value = coalesce(
                 (regexp_match(
                   f.endpoint_url,
                   '/(?:amendment|bill|committee-meeting|committee-print|committee-report|committee|hearing|nomination|treaty)/([0-9]{1,3})(?:/|[?]|$)'
                 ))[1],
                 case when f.category = 'members' then '*' end
               )
           )
         )
     ), response_keys as (
       select distinct
              case when strpos(r.endpoint_url, '?') = 0 then r.endpoint_url
                   else split_part(r.endpoint_url, '?', 1) || '?' ||
                        (select string_agg(part, '&' order by part)
                           from unnest(string_to_array(split_part(r.endpoint_url, '?', 2), '&')) as query_parts(part))
              end as endpoint_key
       from raw_response_current r
     )
     select f.category,
            f.http_status,
            count(distinct f.endpoint_url) as unresolved_urls,
            min(f.failed_at) as first_failure,
            max(f.failed_at) as last_failure
       from failed_keys f
       left join response_keys r on r.endpoint_key = f.endpoint_key
      where r.endpoint_key is null
      group by f.category, f.http_status
      order by f.category, f.http_status;"
  echo "missing_congress_root_partitions"
  # Keep uncrawled partitions distinct from source-empty partitions.  A left
  # join with only an item count treats both cases as zero and previously made
  # an active backfill look like a source-availability gap.
  bicam_psql -At -F '|' -c \
    "with roots(category, node_type, item_key, path) as (
       values
         ('amendments','amendment_list','amendments','amendment'),
         ('bills','bill_list','bills','bill'),
         ('committee-meetings','committee_meeting_list','committeeMeetings','committee-meeting'),
         ('committee-prints','committee_print_list','committeePrints','committee-print'),
         ('committee-reports','committee_report_list','reports','committee-report'),
         ('committees','committee_list','committees','committee'),
         ('hearings','hearing_list','hearings','hearing'),
         ('nominations','nomination_list','nominations','nomination'),
         ('treaties','treaty_list','treaties','treaty')
     ),
     scope as (
       select s.category, regexp_replace(trim(p), '^p', '') as partition_value
       from collection_scope_partitions s
       join (select distinct category from roots) covered using (category)
       cross join lateral regexp_split_to_table(s.partition_values, ',') p
       where s.source = 'congress'
         and s.partition_field = 'congress'
     ),
     observed as (
       select r.category,
              (regexp_match(c.endpoint_url, '.*/' || r.path || '/([0-9]+).*'))[1] as partition_value,
              count(*) as root_response_count,
              sum(jsonb_array_length(coalesce(c.response -> r.item_key, '[]'::jsonb))) as item_count
       from raw_response_current c
       join roots r on r.category = c.category and r.node_type = c.node_type
       where c.source = 'congress'
         and c.endpoint_url ~ '/(bill|amendment|committee-meeting|committee-print|committee-report|committee|hearing|nomination|treaty)/[0-9]+'
       group by 1,2
     )
     select scope.category, string_agg(scope.partition_value, ',' order by scope.partition_value::int)
     from scope
     left join observed using (category, partition_value)
     where observed.category is null
     group by scope.category
     order by scope.category;"
  echo "empty_congress_root_partitions"
  # A root is source-empty only when at least one current response exists and
  # every observed response contains zero items.  This is evidence about the
  # upstream API, not permission to silently shrink the declared 1-119 scope.
  bicam_psql -At -F '|' -c \
    "with roots(category, node_type, item_key, path) as (
       values
         ('amendments','amendment_list','amendments','amendment'),
         ('bills','bill_list','bills','bill'),
         ('committee-meetings','committee_meeting_list','committeeMeetings','committee-meeting'),
         ('committee-prints','committee_print_list','committeePrints','committee-print'),
         ('committee-reports','committee_report_list','reports','committee-report'),
         ('committees','committee_list','committees','committee'),
         ('hearings','hearing_list','hearings','hearing'),
         ('nominations','nomination_list','nominations','nomination'),
         ('treaties','treaty_list','treaties','treaty')
     ),
     scope as (
       select s.category, regexp_replace(trim(p), '^p', '') as partition_value
       from collection_scope_partitions s
       join (select distinct category from roots) covered using (category)
       cross join lateral regexp_split_to_table(s.partition_values, ',') p
       where s.source = 'congress'
         and s.partition_field = 'congress'
     ),
     observed as (
       select r.category,
              (regexp_match(c.endpoint_url, '.*/' || r.path || '/([0-9]+).*'))[1] as partition_value,
              count(*) as root_response_count,
              sum(jsonb_array_length(coalesce(c.response -> r.item_key, '[]'::jsonb))) as item_count
       from raw_response_current c
       join roots r on r.category = c.category and r.node_type = c.node_type
       where c.source = 'congress'
         and c.endpoint_url ~ '/(bill|amendment|committee-meeting|committee-print|committee-report|committee|hearing|nomination|treaty)/[0-9]+'
       group by 1,2
     )
     select scope.category, string_agg(scope.partition_value, ',' order by scope.partition_value::int)
     from scope
     join observed using (category, partition_value)
     where observed.root_response_count > 0
       and coalesce(observed.item_count, 0) = 0
     group by scope.category
     order by scope.category;"
  echo "child_coverage"
  timeout --kill-after=10s 300s uv run python scripts/check_child_coverage.py \
    --output "$report_dir/child-coverage-audit-latest.txt" \
    --history-dir "$report_dir" || \
    echo "child_coverage_exit=$?"
  echo "quality_gate"
  timeout --kill-after=10s 300s uv run python scripts/check_quality_gates.py || \
    echo "quality_gate_exit=$?"
} >"$temporary" 2>&1

mv "$temporary" "$latest"
cp "$latest" "$history"
cat "$latest"
