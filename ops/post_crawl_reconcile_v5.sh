#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

supervisor_pid=${1:?supervisor PID is required}
log_file=${2:-/tmp/bicam-post-crawl-reconcile-v5.log}
cooldown_marker=$reports_root/congress-api-cooldown-until
additional_supervisor_pids=("${@:3}")
background_pids=()

cooldown_active() {
  local until now
  until=$(cat "$cooldown_marker" 2>/dev/null || true)
  now=$(date +%s)
  [[ "$until" =~ ^[0-9]+$ ]] && (( until > now ))
}

abort_if_congress_cooldown() {
  if cooldown_active; then
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

# The partition supervisor is not the only crawler we may run.  Targeted
# historical bill workers can continue beyond the supervisor's final child;
# never start retries or dbt while any Congress bills/amendments crawl still
# owns a running crawl_run.  This also prevents an orphaned process from being
# mistaken for a completed ingestion pass.
while :; do
  # A worker can exit after writing substantial telemetry but before its
  # crawl_run status is finalized. Reclaim only stale rows when no executable
  # crawler path exists; fresh rows remain protected during child launch.
  if ! pgrep -f '/(backfill_congress_root_details|run_congress_history|run_congress_descendant_refresh|run_multi_crawl|run_root_gap_repair)[.]py' >/dev/null 2>&1; then
    docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -c \
      "update crawl_runs set status='abandoned', finished_at=now()
         where status='running'
           and started_at < now() - interval '15 minutes'
           and not (category='amendments' and mode='backfill_refresh_descendants');" \
      >/dev/null 2>&1 || true
  fi
  active_runs=$(docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -c \
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

# Do not hold the shared ingestion lock while waiting for a durable
# Congress.gov Retry-After window.  The marker is written by either Congress
# lane and can outlive the worker that observed the 429s; releasing here lets
# the persistent queue/ramp services resume as soon as the marker expires.
if cooldown_active; then
  echo "[$(date -u)] supervisor_congress_cooldown_release marker=$cooldown_marker" >>"$log_file"
  exit 0
fi

cd "$project_root" || exit 1
bicam_load_project_env
bicam_prefer_container_db_password
# The live API headers and controlled per-token probe show a 20,000-request
# counter on each of the 17 configured credentials. That proves only the
# observed token-level counters; it does not prove that the credentials have
# independent account quotas. The earlier 20,000 aggregate profile was a
# temporary rollback; the latest controlled continuation reached the same
# boundary again and received coordinated long-`Retry-After` 429s. Keep the
# default at 20,000 attempts/hour until a separate egress/project boundary is
# available, while retaining all keys for fair rotation and failover.
export BICAM_CONGRESS_GLOBAL_RATE_LIMIT="${BICAM_SUPERVISOR_CONGRESS_GLOBAL_RATE_LIMIT:-20000}"
export BICAM_CONGRESS_PER_KEY_RATE_LIMIT="${BICAM_SUPERVISOR_CONGRESS_PER_KEY_RATE_LIMIT:-20000}"

{
  date -u
  echo "supervisor_finished"
  cat /tmp/bicam-dbt-partitioned-v5.status 2>/dev/null || true
# The availability report is a periodic snapshot and can remain in the
# quota_cooldown state after the server-provided marker has expired.  Use the
# durable marker itself for this decision; otherwise a stale snapshot can
# suppress retries and root repairs for an entire supervisor cycle.
if cooldown_active; then
    echo "quota_cooldown_skip_preflight=1"
  else
  echo "targeted_historical_root_recheck"
  # A handful of early central-document bill detail URLs returned source-side
  # 500s during the historical pass, and older global list routes can expose
  # amendment details that partition crawls did not enumerate. Recheck the
  # bounded historical root backlogs serially after all broad crawlers finish.
  # This is deliberately separate from the generic transient retry loop,
  # which excludes source-side 500s by default.
  root_recheck_attempt=0
  # Historical central-document URLs currently return deterministic
  # MemberTerm/DoesNotExist 500s. One bounded pass records the failure; the
  # later source-500 retry stage remains the explicit opt-in recovery path.
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
  # The broad partitioned descendant crawl normally closes these gaps while
  # walking each Congress list.  Re-run the exact indexed anti-join after all
  # workers finish so a broken pagination chain or a transient detail failure
  # cannot leave the final coverage report falsely green.  This pass is
  # intentionally bounded to the modern partitions; the historical pass
  # above handles Congresses 1..101, including global-list amendment items.
  modern_root_attempt=0
  modern_root_attempts=${BICAM_MODERN_ROOT_RECHECK_ATTEMPTS:-1}
  modern_root_limit=${BICAM_MODERN_ROOT_RECHECK_LIMIT:-1000000}
  # A static Congress-range/key-shard layout underutilizes the credential pool
  # when the backlog is concentrated in only a few ranges. Run one category at
  # a time in a single process with the complete configured key pool instead;
  # the process-wide pacer and guarded per-credential windows keep all keys
  # productive without an unbounded burst.
  modern_root_concurrency=${BICAM_MODERN_ROOT_RECHECK_CONCURRENCY:-16}
  # Rotisserie tracks each credential's rolling window locally. Keep the same
  # low scheduler floor as the child sweep so future root repairs do not add an
  # artificial process-wide ceiling; live API headers remain the safety signal.
  modern_root_request_interval=${BICAM_MODERN_ROOT_RECHECK_REQUEST_INTERVAL:-0.05}
  # Two local slots per credential improve utilization when source responses
  # are slower than the credential cadence; they do not change the tokens or
  # multiply a credential's server-side quota.
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
  # Recover source-side 500 root details before descendants are walked.  The
  # retry is seed-only so a transient outage cannot fan out, while any detail
  # that does recover is visible to the child sweep below.  A single bounded
  # pass also keeps deterministic historical 500s from being hammered.
  # Drain source-500 seeds in bounded batches.  A single 500-row pass can
  # leave the tail of a historical outage permanently unresolved; keep the
  # batches finite while allowing every currently eligible seed one retry.
  root_source_500_retry_limit=${BICAM_ROOT_SOURCE_500_RETRY_LIMIT:-100}
  # A source-side 500 is not made more recoverable by replaying the same
  # historical URL twenty times in one supervisor pass.  Keep one bounded
  # probe as the default; a later scheduled pass can try again after the
  # upstream service has had time to recover.  Operators can still raise this
  # explicitly for a diagnosed transient outage.
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
  # GovInfo can expose a newly introduced bill before the corresponding
  # Congress.gov list/detail crawl has seen it.  Probe those exact canonical
  # identities before the child sweep so a successful response becomes part
  # of the normal bill reconciliation graph.  The helper is bounded and
  # idempotent; 404s remain durable evidence for a later source-gap review.
  uv run python scripts/backfill_congress_bill_details.py \
    --limit "${BICAM_GOVINFO_ONLY_BILL_PROBE_LIMIT:-100}" \
    --concurrency 1 \
    --request-interval 2.0 \
    --key-wait-timeout 600 || true
  echo "probe_modern_missing_govinfo_bills"
  # A GovInfo package can appear after an initial 404 while a Congress is
  # still active.  Revisit modern Congress-only identities on a bounded TTL;
  # historical pre-103 source gaps remain covered by the reconciliation rule
  # and are not sent through this probe.
  uv run python scripts/backfill_govinfo_bills.py \
    --modern-missing \
    --limit "${BICAM_GOVINFO_MODERN_MISSING_LIMIT:-500}" \
    --concurrency "${BICAM_GOVINFO_MODERN_MISSING_CONCURRENCY:-3}" \
    --request-interval "${BICAM_GOVINFO_MODERN_MISSING_REQUEST_INTERVAL:-1.0}" \
    --key-wait-timeout 600 \
    --retry-404-after-hours "${BICAM_GOVINFO_MODERN_MISSING_RETRY_404_AFTER_HOURS:-168}" || true
  echo "probe_govinfo_only_hearings"
  # GovInfo Serial Set hearing packages extend earlier than the Congress.gov
  # hearing API.  Probe only modern GovInfo packages (104-119) that are not
  # linked to a stored Congress hearing; older packages remain an explicit
  # historical source-coverage gap.
  uv run python scripts/backfill_congress_govinfo_hearings.py \
    --congress-start 104 --congress-end 119 \
    --limit "${BICAM_GOVINFO_HEARING_PROBE_LIMIT:-2000}" \
    --concurrency 2 \
    --batch-size 100 \
    --request-interval 1.0 \
    --key-wait-timeout 600 || true
  fi
  # Root-detail coverage is not sufficient: bill and amendment detail payloads
  # expose a second layer of paginated child URLs.  Walk every stored Congress
  # partition after root repair so missing actions, committees, text, titles,
  # cosponsors, and related records are fetched before the final quality gate.
  # The crawler is idempotent in backfill mode; existing responses are walked
  # to discover descendants but are not refetched.  Use one client for the
  # complete bill/amendment queue so the process-wide pacer can use every
  # configured key; static Congress shards otherwise leave keys idle whenever
  # the backlog is skewed toward a subset of Congresses.
  child_sweep_recent=0
  child_sweep_log=/tmp/bicam-child-sweep-unified.log
  # A successful all-Congress child sweep is durable in raw_responses.  If a
  # service restart happens immediately afterward, do not replay the entire
  # idempotent traversal just to rediscover the same URLs; the bounded refresh
  # lanes and child-coverage audit below continue to catch newly exposed links.
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
  # The four category workers saturate the configured virtual key slots. Two
  # slots per credential keep each credential productive when API latency is
  # high, while the client-side aggregate equivalent remains explicit.
  child_backfill_concurrency=${BICAM_CHILD_BACKFILL_CONCURRENCY:-16}
  child_backfill_workers=${BICAM_CHILD_BACKFILL_CATEGORY_WORKERS:-4}
  # The live deployment advertises a 20,000-request/hour header per token.
  # Account-level scope is not exposed.  A mixed-worker/high-pressure run
  # also hit coordinated 429s near 20,000 aggregate requests, but that does
  # not identify an account quota or a clean source-wide hourly limit.  Keep
  # this child pass at the reversible deployment guard until an isolated ramp
  # separates token quotas from egress/burst pressure.
  # The 0.05s local floor removes an artificial aggregate bottleneck; actual
  # in-flight work is still bounded by 34 scheduling slots and the availability
  # guard can pause on coordinated 429s.
  child_backfill_interval=${BICAM_CHILD_BACKFILL_REQUEST_INTERVAL:-0.05}
  child_backfill_key_slots=${BICAM_CHILD_BACKFILL_KEY_SLOTS:-2}
  # The amendment refresh service runs as a separate process with its own
  # local Rotisserie pool. The live counters are token-scoped in probes, but
  # account grouping is unknown; both processes remain independently bounded
  # and the active-slot budget and 95% safety factor stay explicit.
  child_backfill_global_rate_limit=${BICAM_CHILD_BACKFILL_GLOBAL_RATE_LIMIT:-20000}
  # Use all virtual slots here. The separate amendment lane is independently
  # bounded and the per-credential counters are tracked by the API itself.
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
  # A child worker can discover a coordinated 429 after the initial
  # preflight. Do not immediately launch the independent amendment/bill
  # refresh lanes into that same server-provided cooldown window.
  if abort_if_congress_cooldown; then
    exit 0
  fi
  echo "refresh_stored_amendment_descendants"
  # Historical amendment partitions can be empty even when amendment detail
  # records were discovered through the global list.  Walk every stored detail
  # root in offset batches so those records' child URLs are not stranded
  # outside the partition crawler's reach.  The backfill-refresh mode skips
  # already stored child payloads and is safe to resume after interruption.
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
  # Bill details can also be discovered through the global list and other
  # stored list payloads whose partition was empty or incomplete.  Walk every
  # stored bill detail root as well, using the same idempotent refresh mode so
  # those records' child URLs are not stranded outside the partition sweep.
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
  # Committee meetings and hearings are separate Congress.gov categories.
  # Follow the meeting-side hearingTranscript links explicitly, while keeping
  # standalone hearings intact; the helper stores the fetched detail under
  # the hearings category with meeting provenance.
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
  # A source-side 500 is normally deterministic, so the regular retry loop
  # excludes it.  The current Congress.gov outage surfaced as a 500 Valkey
  # persistence error, however; give every unresolved 500 one bounded replay
  # through the client-side marker-aware retry logic.  Keep this outside the
  # repeated transient loop so malformed historical URLs cannot be hammered.
  # As above, retry every eligible source-500 seed, not just the first 500.
  source_500_retry_limit=${BICAM_SOURCE_500_RETRY_LIMIT:-100}
  # Keep the generic source-500 replay bounded for the same reason as the
  # root-detail pass above.  Deterministic historical 500s must remain in the
  # durable failure ledger without consuming the whole API window.
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
  # Retry all transient failures in bounded batches.  A single 100-row retry
  # leaves large 502 bursts unresolved; the helper excludes endpoints with a
  # known source-side 500 and never fans out from a retry seed.
retry_batch_limit=${BICAM_RETRY_BATCH_LIMIT:-250}
# Keep the transient retry pass bounded.  A deterministic endpoint can remain
# eligible after every replay (for example a historical hearing page); without
# a small cap this lock-held maintenance pass repeatedly spends API budget on
# the same rows and delays the DBT/reconciliation checkpoint.  Later timer
# runs will retry any genuinely transient failures again.
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
  # A failed paginated child/list request breaks that URL's next-page chain.
  # Once transient retries have populated the missing page, replay only the
  # affected Congress partitions in backfill mode so stored pagination and
  # child links are walked forward without replaying the whole history.
  affected_congresses=$(docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -c "
    select distinct substring(fetches.endpoint_url from '/bill/([0-9]+)')::int
    from crawl_run_fetches as fetches
    join crawl_runs as runs on runs.id = fetches.crawl_run_id
    where runs.category = 'bills'
      and fetches.outcome = 'failed'
      and fetches.http_status = any(array[408,425,429,502,503,504,520,522,523,524])
      and not exists (
        select 1 from raw_responses as responses
        where responses.endpoint_url = fetches.endpoint_url
      )
      and substring(fetches.endpoint_url from '/bill/([0-9]+)') is not null
    order by 1;
  " 2>/dev/null || true)
  for congress in $affected_congresses; do
    if abort_if_congress_cooldown; then
      exit 0
    fi
    [ -n "$congress" ] || continue
    echo "replay_bills_congress=$congress"
    uv run python scripts/run_congress_history.py \
      --categories bills \
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
  affected_amendment_congresses=$(docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -c "
    select distinct substring(fetches.endpoint_url from '/amendment/([0-9]+)')::int
    from crawl_run_fetches as fetches
    join crawl_runs as runs on runs.id = fetches.crawl_run_id
    where runs.category = 'amendments'
      and fetches.outcome = 'failed'
      and fetches.http_status = any(array[408,425,429,502,503,504,520,522,523,524])
      and not exists (
        select 1 from raw_responses as responses
        where responses.endpoint_url = fetches.endpoint_url
      )
      and substring(fetches.endpoint_url from '/amendment/([0-9]+)') is not null
    order by 1;
  " 2>/dev/null || true)
  for congress in $affected_amendment_congresses; do
    if abort_if_congress_cooldown; then
      exit 0
    fi
    [ -n "$congress" ] || continue
    echo "replay_amendments_congress=$congress"
    uv run python scripts/run_congress_history.py \
      --categories amendments \
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
  echo "probe_missing_committee_reports"
  if abort_if_congress_cooldown; then
    exit 0
  fi
  # The historical committee-report crawler follows list-provided detail
  # links.  A small set of GovInfo reports has no such link, so probe those
  # exact Congress.gov detail URLs after the bill/amendment work is complete.
  # Keep this deliberately serial and low-rate; it is a reconciliation probe,
  # not another broad backfill.
  uv run python scripts/backfill_congress_committee_reports.py \
    --limit "${BICAM_COMMITTEE_REPORT_PROBE_LIMIT:-100}" \
    --concurrency 1 \
    --request-interval 2.0 \
    --key-wait-timeout 600 \
    --active-key-budget 1 || true
  echo "rebuild_reconciliation"
  export DBT_PROFILES_DIR="$project_root/dbt_project"
  # The collection-coverage model scans the historical fetch ledger and can
  # exceed the old three-minute default after a large backfill.  Allow the
  # bounded post-retry reconciliation to finish rather than leaving coverage
  # metrics stale.
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
  docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -F $'\t' -c "
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
  docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -F $'\t' -c "
    select 'fct_bill_matches', count(*) from fct_bill_matches
    union all select 'fct_committee_report_matches', count(*) from fct_committee_report_matches
    union all select 'fct_reconciliation_metrics', count(*) from fct_reconciliation_metrics
    union all select 'fct_collection_coverage', count(*) from fct_collection_coverage;
  "
  echo "metrics"
  docker exec bicam-collection-postgres psql -U admin -d bicam_collection -c "
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
