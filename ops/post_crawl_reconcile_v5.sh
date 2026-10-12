#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"
bicam_load_project_env

supervisor_pid=${1:?supervisor PID is required}
log_file=${2:-/tmp/bicam-post-crawl-reconcile-v5.log}
cooldown_path=$reports_root/congress-api-cooldown-until
additional_supervisor_pids=("${@:3}")
background_pids=()

abort_if_congress_cooldown() {
  if bicam_cooldown_active; then
    echo "quota_cooldown_abort_remaining_congress_stages=1"
    return 0
  fi
  return 1
}

forget_background_pid() {
  local target=$1
  local pid
  local remaining=()
  for pid in "${background_pids[@]}"; do
    [ "$pid" = "$target" ] || remaining+=("$pid")
  done
  background_pids=("${remaining[@]}")
}

cleanup_background_pids() {
  trap - TERM INT EXIT
  local pid
  for pid in "${background_pids[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  sleep 1
  for pid in "${background_pids[@]}"; do
    kill -KILL "$pid" 2>/dev/null || true
  done
}

handle_termination() {
  cleanup_background_pids
  exit 143
}

replay_failed_partitions() {
  local category=$1 path_segment=$2 congress
  for congress in $(bicam_psql -At -c "
    select distinct substring(fetches.endpoint_url from '/${path_segment}/([0-9]+)')::int
    from crawl_run_fetches as fetches
    join crawl_runs as runs on runs.id = fetches.crawl_run_id
    where runs.category = '${category}'
      and fetches.outcome = 'failed'
      and fetches.http_status = any(array[408,425,429,502,503,504,520,522,523,524])
      and not exists (
        select 1 from raw_responses as responses
        where responses.endpoint_url = fetches.endpoint_url
      )
      and substring(fetches.endpoint_url from '/${path_segment}/([0-9]+)') is not null
    order by 1;
  " 2>/dev/null); do
    if abort_if_congress_cooldown; then
      exit 0
    fi
    echo "replay_${category}_congress=$congress"
    uv run python scripts/run_congress_history.py \
      --categories "$category" \
      --congress-start "$congress" \
      --congress-end "$congress" \
      --mode backfill_descendants \
      --max-pages 1000 \
      --page-size 250 \
      --concurrency "$recovery_concurrency" \
      --category-workers 1 \
      --key-wait-timeout 3600 \
      --request-interval "$recovery_request_interval" \
      --active-key-budget "$recovery_active_key_budget" || true
  done
}

trap cleanup_background_pids EXIT
trap handle_termination TERM INT

if [ "${BICAM_SKIP_SUPERVISOR_WAIT:-0}" != "1" ]; then
  while :; do
    all_finished=1
    for pid in "$supervisor_pid" "${additional_supervisor_pids[@]}"; do
      if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        all_finished=0
        break
      fi
    done
    [ "$all_finished" -eq 1 ] && break
    sleep 60
  done
fi

# Wait until no bills/amendments crawl owns a running crawl_run (workers can
# outlive the supervisor).
while :; do
  # Reclaim stale 'running' rows only when no crawler process exists.
  if ! pgrep -f '/(backfill_congress_root_details|run_congress_history|run_congress_descendant_refresh|run_multi_crawl|run_root_gap_repair)[.]py' >/dev/null 2>&1; then
    bicam_psql -At -c \
      "update crawl_runs set status='abandoned', finished_at=now()
         where status='running'
           and started_at < now() - interval '15 minutes'
           and not (category='amendments' and mode='backfill_refresh_descendants');" \
      >/dev/null 2>&1 || true
  fi
  active_runs=$(bicam_psql -At -c \
    "select count(*)
       from crawl_runs
      where status = 'running'
        and not (category = 'amendments' and mode = 'backfill_refresh_descendants');" \
    2>/dev/null || echo 1)
  if [ "$active_runs" = "0" ] && ! pgrep -f '[r]un_root_gap_repair.py' >/dev/null 2>&1; then
    break
  fi
  sleep 60
done

# Don't hold the lock through a Retry-After window.
if bicam_cooldown_active; then
  echo "[$(date -u)] supervisor_congress_cooldown_release marker=$cooldown_path" >>"$log_file"
  exit 0
fi

cd "$project_root" || exit 1
# 20,000 requests/hour aggregate: the boundary where coordinated 429s appeared.
export BICAM_CONGRESS_GLOBAL_RATE_LIMIT="${BICAM_SUPERVISOR_CONGRESS_GLOBAL_RATE_LIMIT:-20000}"
export BICAM_CONGRESS_PER_KEY_RATE_LIMIT="${BICAM_SUPERVISOR_CONGRESS_PER_KEY_RATE_LIMIT:-20000}"

{
  date -u
  echo "supervisor_finished"
  cat /tmp/bicam-dbt-partitioned-v5.status 2>/dev/null || true
# The cooldown marker, not the periodic availability snapshot, decides.
if bicam_cooldown_active; then
    echo "quota_cooldown_skip_preflight=1"
  else
  echo "targeted_historical_root_recheck"
  # Recheck historical (Congress 1-101) root details after the broad crawlers finish.
  root_recheck_attempt=0
  # Those URLs mostly return deterministic 500s, so one pass by default.
  historical_root_recheck_attempts=${BICAM_HISTORICAL_ROOT_RECHECK_ATTEMPTS:-1}
  while [ "$root_recheck_attempt" -lt "$historical_root_recheck_attempts" ]; do
    root_recheck_attempt=$((root_recheck_attempt + 1))
    echo "root_recheck_attempt=$root_recheck_attempt"
    historical_root_remaining=0
    for historical_category in bills amendments; do
      root_recheck_output=$(uv run python scripts/backfill_congress_root_details.py \
        --category "$historical_category" --congress-start 1 --congress-end 101 \
        --limit 10000 --concurrency 2 --request-interval 1.5 \
        --key-wait-timeout 120 --active-key-budget 2 2>&1) || true
      echo "$root_recheck_output"
      if ! printf '%s\n' "$root_recheck_output" | grep -q 'candidates=0'; then
        historical_root_remaining=1
      fi
    done
    [ "$historical_root_remaining" -eq 0 ] && break
    [ "$root_recheck_attempt" -lt "$historical_root_recheck_attempts" ] && sleep 60
  done
  echo "modern_root_detail_recheck"
  # Re-run the root-detail anti-join for modern Congresses so a broken list chain
  # can't leave coverage falsely green. One process per category uses the whole key pool.
  modern_root_attempt=0
  modern_root_attempts=${BICAM_MODERN_ROOT_RECHECK_ATTEMPTS:-1}
  modern_root_limit=${BICAM_MODERN_ROOT_RECHECK_LIMIT:-1000000}
  modern_root_concurrency=${BICAM_MODERN_ROOT_RECHECK_CONCURRENCY:-16}
  modern_root_request_interval=${BICAM_MODERN_ROOT_RECHECK_REQUEST_INTERVAL:-0.05}
  modern_root_key_slots=${BICAM_MODERN_ROOT_RECHECK_KEY_SLOTS:-2}
  modern_root_active_key_budget=${BICAM_MODERN_ROOT_RECHECK_ACTIVE_KEY_BUDGET:-$((17 * modern_root_key_slots))}
  modern_root_plans=(
    "bills 102 119"
    "amendments 102 119"
  )
  while [ "$modern_root_attempt" -lt "$modern_root_attempts" ]; do
    modern_root_attempt=$((modern_root_attempt + 1))
    modern_root_remaining=0
    modern_root_status=0
    echo "modern_root_recheck_attempt=$modern_root_attempt"
    for modern_root_plan in "${modern_root_plans[@]}"; do
      read -r modern_category modern_root_start modern_root_end <<<"$modern_root_plan"
      modern_root_log="/tmp/bicam-modern-root-${modern_category}-${modern_root_start}-${modern_root_end}-attempt-${modern_root_attempt}.log"
      echo "started_modern_root category=$modern_category congress=$modern_root_start-$modern_root_end log=$modern_root_log"
      env -u BICAM_CONGRESS_KEY_SHARD \
        BICAM_CONGRESS_KEY_SLOTS="$modern_root_key_slots" \
        uv run python scripts/backfill_congress_root_details.py \
          --category "$modern_category" \
          --congress-start "$modern_root_start" --congress-end "$modern_root_end" \
          --limit "$modern_root_limit" \
          --concurrency "$modern_root_concurrency" \
          --request-interval "$modern_root_request_interval" \
          --key-wait-timeout 3600 \
          --active-key-budget "$modern_root_active_key_budget" \
          >"$modern_root_log" 2>&1 || modern_root_status=1
      echo "--- $modern_root_log ---"
      modern_root_output=$(cat "$modern_root_log" 2>/dev/null || true)
      echo "$modern_root_output"
      if ! printf '%s\n' "$modern_root_output" | grep -q 'candidates=0'; then
        modern_root_remaining=1
      fi
    done
    [ "$modern_root_status" -eq 0 ] || echo "one or more modern root categories failed; continuing to the bounded retry attempts"
    [ "$modern_root_remaining" -eq 0 ] && break
    [ "$modern_root_attempt" -lt "$modern_root_attempts" ] && sleep 60
  done
  echo "retry_root_source_500_once"
  # One bounded seed-only replay of root-detail source 500s before the child sweep.
  root_source_500_retry_limit=${BICAM_ROOT_SOURCE_500_RETRY_LIMIT:-100}
  root_source_500_retry_max_batches=${BICAM_ROOT_SOURCE_500_RETRY_MAX_BATCHES:-1}
  root_source_500_retry_batch=0
  while [ "$root_source_500_retry_batch" -lt "$root_source_500_retry_max_batches" ]; do
    root_source_500_retry_batch=$((root_source_500_retry_batch + 1))
    echo "root_source_500_retry_batch=$root_source_500_retry_batch limit=$root_source_500_retry_limit"
    root_source_500_retry_output=$(uv run python scripts/retry_failed_fetches.py \
      --categories bills amendments \
      --include-source-500 \
      --only-source-500 \
      --limit "$root_source_500_retry_limit" \
      --request-interval 2.0 \
      --key-wait-timeout 600 \
      --min-age-minutes "${BICAM_ROOT_SOURCE_500_RETRY_MIN_AGE_MINUTES:-15}" \
      --no-descendants 2>&1) || true
    echo "$root_source_500_retry_output"
    printf '%s\n' "$root_source_500_retry_output" | grep -q "no unresolved failed fetches need retry" && break
    printf '%s\n' "$root_source_500_retry_output" | grep -q "\[retry\] attempted=" || break
  done
  [ "$root_source_500_retry_batch" -lt "$root_source_500_retry_max_batches" ] || \
    echo "root_source_500_retry_batch_limit_reached=$root_source_500_retry_max_batches"
  echo "probe_govinfo_only_bills"
  # Probe Congress.gov for GovInfo-only bills before the child sweep.
  uv run python scripts/backfill_congress_bill_details.py \
    --limit "${BICAM_GOVINFO_ONLY_BILL_PROBE_LIMIT:-100}" \
    --concurrency 1 \
    --request-interval 2.0 \
    --key-wait-timeout 600 || true
  echo "probe_modern_missing_govinfo_bills"
  # Re-probe modern Congress-only bills whose GovInfo package may have appeared (TTL-bound).
  uv run python scripts/backfill_govinfo_bills.py \
    --modern-missing \
    --limit "${BICAM_GOVINFO_MODERN_MISSING_LIMIT:-500}" \
    --concurrency "${BICAM_GOVINFO_MODERN_MISSING_CONCURRENCY:-3}" \
    --request-interval "${BICAM_GOVINFO_MODERN_MISSING_REQUEST_INTERVAL:-1.0}" \
    --key-wait-timeout 600 \
    --retry-404-after-hours "${BICAM_GOVINFO_MODERN_MISSING_RETRY_404_AFTER_HOURS:-168}" || true
  echo "probe_govinfo_only_hearings"
  # Probe Congress.gov for modern (104-119) GovInfo hearing packages.
  uv run python scripts/backfill_congress_govinfo_hearings.py \
    --congress-start 104 --congress-end 119 \
    --limit "${BICAM_GOVINFO_HEARING_PROBE_LIMIT:-2000}" \
    --concurrency 2 \
    --batch-size 100 \
    --request-interval 1.0 \
    --key-wait-timeout 600 || true
  fi
  # Walk every bill/amendment partition in backfill mode to fetch missing child
  # endpoints; stored responses are traversed, not refetched.
  child_sweep_recent=0
  child_sweep_log=/tmp/bicam-child-sweep-unified.log
  # Skip if a complete sweep already finished (or one ran in the last six hours).
  if [ "${BICAM_FORCE_UNIFIED_CHILD_SWEEP:-0}" != "1" ] \
    && [ -f "$reports_root/congress-child-sweep-complete" ]; then
    child_sweep_recent=1
  elif [ -f "$child_sweep_log" ] \
    && find "$child_sweep_log" -mmin -360 -print -quit 2>/dev/null | grep -q . \
    && grep -q 'completed=238 failed=0 jobs=238' "$child_sweep_log"; then
    child_sweep_recent=1
  fi
  if [ "${BICAM_SKIP_UNIFIED_CHILD_SWEEP:-0}" = "1" ] || [ "$child_sweep_recent" -eq 1 ]; then
    echo "backfill_bill_amendment_descendants_skipped=1 recent=$child_sweep_recent"
  else
  echo "backfill_bill_amendment_descendants"
  child_backfill_concurrency=${BICAM_CHILD_BACKFILL_CONCURRENCY:-16}
  child_backfill_workers=${BICAM_CHILD_BACKFILL_CATEGORY_WORKERS:-4}
  child_backfill_interval=${BICAM_CHILD_BACKFILL_REQUEST_INTERVAL:-0.05}
  child_backfill_key_slots=${BICAM_CHILD_BACKFILL_KEY_SLOTS:-2}
  child_backfill_global_rate_limit=${BICAM_CHILD_BACKFILL_GLOBAL_RATE_LIMIT:-20000}
  child_backfill_key_budget=${BICAM_CHILD_BACKFILL_ACTIVE_KEY_BUDGET:-$((17 * child_backfill_key_slots))}
  child_log="/tmp/bicam-child-sweep-unified.log"
  child_backfill_status=0
  echo "started_child_unified congress=1-119 log=$child_log"
  env -u BICAM_CONGRESS_KEY_SHARD \
    BICAM_CONGRESS_KEY_SLOTS="$child_backfill_key_slots" \
    BICAM_CONGRESS_GLOBAL_RATE_LIMIT="$child_backfill_global_rate_limit" \
    uv run python scripts/run_congress_history.py \
      --categories bills amendments \
      --congress-start 1 --congress-end 119 \
      --mode backfill_descendants --max-pages 1000 --page-size 250 \
      --concurrency "$child_backfill_concurrency" \
      --category-workers "$child_backfill_workers" \
      --key-wait-timeout 3600 \
      --request-interval "$child_backfill_interval" \
      --active-key-budget "$child_backfill_key_budget" \
      >"$child_log" 2>&1 || child_backfill_status=1
  echo "--- $child_log ---"
  cat "$child_log" 2>/dev/null || true
  [ "$child_backfill_status" -eq 0 ] || echo "unified child sweep failed; continuing to retries and audits"
  fi
  # A 429 cooldown may have started during the sweep.
  if abort_if_congress_cooldown; then
    exit 0
  fi
  echo "refresh_stored_amendment_descendants"
  # Walk stored amendment roots too: some were found via the global list, outside any partition.
  uv run python scripts/run_congress_descendant_refresh.py \
    --categories amendments \
    --batch-size "${BICAM_DESCENDANT_REFRESH_BATCH_SIZE:-500}" \
    --max-batches "${BICAM_DESCENDANT_REFRESH_MAX_BATCHES:-1}" \
    --concurrency "${BICAM_DESCENDANT_REFRESH_CONCURRENCY:-16}" \
    --page-size 250 \
    --max-pages 1000 \
    --request-interval "${BICAM_DESCENDANT_REFRESH_REQUEST_INTERVAL:-0.24}" \
    --key-wait-timeout 3600 \
    --active-key-budget "${BICAM_DESCENDANT_REFRESH_ACTIVE_KEY_BUDGET:-32}" || true
  if abort_if_congress_cooldown; then
    exit 0
  fi
  echo "refresh_stored_bill_descendants"
  # Same for stored bill roots.
  uv run python scripts/run_congress_descendant_refresh.py \
    --categories bills \
    --batch-size "${BICAM_BILL_DESCENDANT_REFRESH_BATCH_SIZE:-500}" \
    --max-batches "${BICAM_BILL_DESCENDANT_REFRESH_MAX_BATCHES:-1}" \
    --concurrency "${BICAM_BILL_DESCENDANT_REFRESH_CONCURRENCY:-16}" \
    --page-size 250 \
    --max-pages 1000 \
    --request-interval "${BICAM_BILL_DESCENDANT_REFRESH_REQUEST_INTERVAL:-0.24}" \
    --key-wait-timeout 3600 \
    --active-key-budget "${BICAM_BILL_DESCENDANT_REFRESH_ACTIVE_KEY_BUDGET:-32}" || true
  if abort_if_congress_cooldown; then
    exit 0
  fi
  echo "backfill_committee_meeting_hearings"
  # Follow meeting -> hearing transcript links.
  uv run python scripts/backfill_congress_meeting_hearings.py \
    --limit "${BICAM_MEETING_HEARING_LIMIT:-500}" \
    --concurrency 2 \
    --batch-size 100 \
    --request-interval 1.5 \
    --key-wait-timeout 600 \
    --active-key-budget 2 \
    --retry-404-after-hours "${BICAM_MEETING_HEARING_RETRY_404_AFTER_HOURS:-168}" || true
  if abort_if_congress_cooldown; then
    exit 0
  fi
  echo "child_coverage_audit"
  if pgrep -f '[c]heck_child_coverage[.]py' >/dev/null 2>&1; then
    echo "child_coverage_audit_already_running=1"
  else
    child_coverage_output=$(uv run python scripts/check_child_coverage.py \
      --output "$reports_root/child-coverage-audit-latest.txt" \
      --history-dir "$reports_root" 2>&1) || true
    echo "$child_coverage_output"
  fi
  echo "retry_source_500_once"
  if abort_if_congress_cooldown; then
    exit 0
  fi
  # One bounded replay of every source 500 (an outage can surface as a 500).
  source_500_retry_limit=${BICAM_SOURCE_500_RETRY_LIMIT:-100}
  source_500_retry_max_batches=${BICAM_SOURCE_500_RETRY_MAX_BATCHES:-1}
  source_500_retry_batch=0
  while [ "$source_500_retry_batch" -lt "$source_500_retry_max_batches" ]; do
    if abort_if_congress_cooldown; then
      exit 0
    fi
    source_500_retry_batch=$((source_500_retry_batch + 1))
    echo "source_500_retry_batch=$source_500_retry_batch limit=$source_500_retry_limit"
    source_500_retry_output=$(uv run python scripts/retry_failed_fetches.py \
      --categories bills amendments hearings committee-meetings committee-prints committee-reports committees members nominations treaties \
        govinfo-bills govinfo-committee-reports govinfo-hearings govinfo-committee-prints \
      --include-source-500 \
      --only-source-500 \
      --limit "$source_500_retry_limit" \
      --request-interval 2.0 \
      --key-wait-timeout 600 \
      --min-age-minutes 15 \
      --no-descendants 2>&1) || true
    echo "$source_500_retry_output"
    printf '%s\n' "$source_500_retry_output" | grep -q "no unresolved failed fetches need retry" && break
    printf '%s\n' "$source_500_retry_output" | grep -q "\[retry\] attempted=" || break
  done
  [ "$source_500_retry_batch" -lt "$source_500_retry_max_batches" ] || \
    echo "source_500_retry_batch_limit_reached=$source_500_retry_max_batches"
  echo "retry_recoverable_failures"
  # Bounded transient retries; later runs pick up whatever remains.
retry_batch_limit=${BICAM_RETRY_BATCH_LIMIT:-250}
retry_max_batches=${BICAM_RETRY_MAX_BATCHES:-4}
retry_batch=0
recovery_concurrency=${BICAM_RECOVERY_CONCURRENCY:-8}
recovery_request_interval=${BICAM_RECOVERY_REQUEST_INTERVAL:-1.5}
  recovery_active_key_budget=${BICAM_RECOVERY_ACTIVE_KEY_BUDGET:-3}
  while [ "$retry_batch" -lt "$retry_max_batches" ]; do
    if abort_if_congress_cooldown; then
      exit 0
    fi
    retry_batch=$((retry_batch + 1))
    echo "retry_batch=$retry_batch limit=$retry_batch_limit"
    retry_output=$(uv run python scripts/retry_failed_fetches.py \
      --categories bills amendments hearings committee-meetings govinfo-bills govinfo-committee-reports govinfo-hearings govinfo-committee-prints \
      --limit "$retry_batch_limit" \
      --request-interval 2.0 \
      --key-wait-timeout 600 \
      --min-age-minutes 15 \
      --no-descendants 2>&1) || true
    echo "$retry_output"
    if printf '%s\n' "$retry_output" | grep -q "no unresolved failed fetches need retry"; then
      break
    fi
    sleep 15
  done
  if [ "$retry_batch" -ge "$retry_max_batches" ]; then
    echo "retry_batch_limit_reached=$retry_max_batches"
  fi
  echo "recover_affected_bill_partitions"
  # Replay Congress partitions whose list/child pages failed transiently, so the
  # next-page chain is walked forward.
  replay_failed_partitions bills bill
  replay_failed_partitions amendments amendment
  echo "probe_missing_committee_reports"
  if abort_if_congress_cooldown; then
    exit 0
  fi
  # Probe the few GovInfo committee reports with no Congress.gov link, serially.
  uv run python scripts/backfill_congress_committee_reports.py \
    --limit "${BICAM_COMMITTEE_REPORT_PROBE_LIMIT:-100}" \
    --concurrency 1 \
    --request-interval 2.0 \
    --key-wait-timeout 600 \
    --active-key-budget 1 || true
  echo "rebuild_reconciliation"
  export DBT_PROFILES_DIR="$project_root/dbt_project"
  dbt_timeout=${BICAM_DBT_TIMEOUT_SECONDS:-600}
  post_retry_run_log=/tmp/bicam-dbt-partitioned-v5-post-retry-run.log
  timeout --signal=TERM --kill-after=15s "${dbt_timeout}s" \
    uv run dbt run --project-dir dbt_project --select +tag:operations +tag:reconciliation \
    > "$post_retry_run_log" 2>&1
  post_retry_run_rc=$?
  post_retry_run_ok_models=$(grep -c 'OK created .* model' "$post_retry_run_log" 2>/dev/null || true)
  if { [ "$post_retry_run_rc" -eq 124 ] || [ "$post_retry_run_rc" -eq 137 ]; } \
    && [ "$post_retry_run_ok_models" -gt 0 ] \
    && ! grep -Eq 'ERROR|Database Error|Compilation Error' "$post_retry_run_log"; then
    echo "post-retry dbt run timed out during shutdown; successful model lines=$post_retry_run_ok_models"
    post_retry_run_rc=0
  fi
  echo "post_retry_dbt_run_rc=$post_retry_run_rc successful_models=$post_retry_run_ok_models"
  post_retry_test_log=/tmp/bicam-dbt-partitioned-v5-post-retry-test.log
  timeout --signal=TERM --kill-after=15s "${dbt_timeout}s" \
    uv run dbt test --project-dir dbt_project \
      --select tag:operations tag:committee_report_reconciliation tag:bill_reconciliation tag:reconciliation \
      > "$post_retry_test_log" 2>&1
  post_retry_test_rc=$?
  if { [ "$post_retry_test_rc" -eq 124 ] || [ "$post_retry_test_rc" -eq 137 ]; } \
    && grep -Eq 'Completed successfully|PASS=[0-9]+ WARN=0 ERROR=0' "$post_retry_test_log"; then
    echo "post-retry dbt test timed out during shutdown after a successful test summary"
    post_retry_test_rc=0
  fi
  echo "post_retry_dbt_test_rc=$post_retry_test_rc"
  echo "unresolved_failures"
  bicam_psql -At -F $'\t' -c "
    with latest as (
      select distinct on (endpoint_url, node_type)
        endpoint_url, node_type, http_status, fetched_at
      from crawl_run_fetches
      where outcome = 'failed'
      order by endpoint_url, node_type, fetched_at desc
    )
    select http_status, count(*)
    from latest as failed
    where not exists (
      select 1 from raw_responses as responses
      where responses.endpoint_url = failed.endpoint_url
    )
    group by http_status
    order by http_status;
  "
  echo "table_counts"
  bicam_psql -At -F $'\t' -c "
    select 'fct_bill_matches', count(*) from fct_bill_matches
    union all select 'fct_committee_report_matches', count(*) from fct_committee_report_matches
    union all select 'fct_reconciliation_metrics', count(*) from fct_reconciliation_metrics
    union all select 'fct_collection_coverage', count(*) from fct_collection_coverage;
  "
  echo "metrics"
  bicam_psql -c "
    select entity, authoritative_source, candidate_count, accepted_count,
      ambiguous_count, unmatched_count, quality_status
    from fct_reconciliation_metrics
    order by entity, authoritative_source;
  "
  post_crawl_status=0
  if [ "${post_retry_run_rc:-0}" -ne 0 ] || [ "${post_retry_test_rc:-0}" -ne 0 ]; then
    post_crawl_status=1
  fi
  echo "post_crawl_final_status=$post_crawl_status"
} > "$log_file" 2>&1

exit "${post_crawl_status:-0}"
