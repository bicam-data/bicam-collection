#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

# Bounded per-key Congress.gov rate experiment (disabled). Paces requests at the
# sum of the per-key windows and stops the worker on a small coordinated-429 signal.

lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
log_path="$reports_root/congress-per-key-ramp.log"
worker_log="$reports_root/congress-per-key-ramp-worker.log"
checkpoint_prefix="$reports_root/congress-per-key-ramp-checkpoint"
cooldown_path="$reports_root/congress-api-cooldown-until"

global_rate_limit=${BICAM_RAMP_GLOBAL_RATE_LIMIT:-340000}
per_key_rate_limit=${BICAM_RAMP_PER_KEY_RATE_LIMIT:-20000}
request_interval=${BICAM_RAMP_REQUEST_INTERVAL:-0.10}
active_key_budget=${BICAM_RAMP_ACTIVE_KEY_BUDGET:-34}
concurrency=${BICAM_RAMP_CONCURRENCY:-34}
category_workers=${BICAM_RAMP_CATEGORY_WORKERS:-4}
max_new_429=${BICAM_RAMP_MAX_NEW_429:-3}
lock_wait_seconds=${BICAM_RAMP_LOCK_WAIT_SECONDS:-300}
ranges=${BICAM_RAMP_CONGRESS_RANGES:-105-119,1-104}
ramp_mode=${BICAM_RAMP_MODE:-refresh}
batch_size=${BICAM_RAMP_BATCH_SIZE:-500}
max_batches=${BICAM_RAMP_MAX_BATCHES:-25}
cursor_path=${BICAM_RAMP_CURSOR_PATH:-$reports_root/congress-descendant-refresh-cursor.json}

mkdir -p "$reports_root"

if bicam_cooldown_active; then
  bicam_wait_for_cooldown congress_per_key_ramp_cooldown_sleep
fi

if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
  echo "[$(date -u)] congress_per_key_ramp_lock_timeout wait_seconds=$lock_wait_seconds" >>"$log_path"
  exit 0
fi

# The cooldown may have started while waiting for the lock.
if bicam_cooldown_active; then
  flock -u 9
  bicam_wait_for_cooldown congress_per_key_ramp_cooldown_sleep
fi

cd "$project_root" || exit 1
bicam_load_project_env

started_at=$(date -u '+%Y-%m-%d %H:%M:%S%z')
started_epoch=$(date +%s)
echo "[$(date -u)] congress_per_key_ramp_started global_rate_limit=$global_rate_limit request_interval=$request_interval max_batches=$max_batches cursor_path=$cursor_path ranges=$ranges" >>"$log_path"

export BICAM_CONGRESS_GLOBAL_RATE_LIMIT="$global_rate_limit"
export BICAM_CONGRESS_PER_KEY_RATE_LIMIT="$per_key_rate_limit"
export BICAM_CONGRESS_KEY_SLOTS="${BICAM_RAMP_KEY_SLOTS:-2}"

write_checkpoint() {
  local label="$1"
  local status="$2"
  local stamp
  stamp=$(date -u '+%Y%m%dT%H%M%SZ')
  local attempts
  local h2xx
  local h429
  # Use the crawl_run_fetches(crawl_run_id) index rather than scanning by fetched_at.
  attempts=$(bicam_db_count "select coalesce(sum(coalesce(cf.attempt_count,0)),0) from crawl_run_fetches cf join crawl_runs cr on cr.id=cf.crawl_run_id where cr.started_at >= '${started_at}' and cf.api_key_name like 'congressional%';")
  h2xx=$(bicam_db_count "select count(*) from crawl_run_fetches cf join crawl_runs cr on cr.id=cf.crawl_run_id where cr.started_at >= '${started_at}' and cf.api_key_name like 'congressional%' and cf.http_status between 200 and 299;")
  h429=$(bicam_db_count "select count(*) from crawl_run_fetches cf join crawl_runs cr on cr.id=cf.crawl_run_id where cr.started_at >= '${started_at}' and cf.api_key_name like 'congressional%' and cf.http_status=429;")
  {
    echo "# Congress per-key ramp checkpoint"
    echo
    echo "Checkpoint: $(date -u '+%Y-%m-%d %H:%M UTC')"
    echo
    echo "- phase: $label"
    echo "- status: $status"
    echo "- local scheduler pressure ceiling: $global_rate_limit attempts/hour (not an account quota or burst target)"
    echo "- per-credential window: $per_key_rate_limit attempts/hour (shared across virtual slots)"
    echo "- process pacer: ${request_interval}s (target ~$(awk -v i="$request_interval" 'BEGIN { if (i > 0) printf "%.0f", 3600/i; else print "unbounded" }') requests/hour)"
    echo "- key slots: ${BICAM_CONGRESS_KEY_SLOTS}"
    echo "- attempts since ramp start: ${attempts:-0}"
    echo "- successful HTTP responses: ${h2xx:-0}"
    echo "- HTTP 429 responses: ${h429:-0}"
    echo "- source run start: $started_at"
    echo
    echo "The database and raw-response archive remain authoritative; this file is a durable operational checkpoint."
  } >"$checkpoint_prefix-$stamp.md"
  echo "[$(date -u)] congress_per_key_ramp_checkpoint phase=$label status=$status attempts=${attempts:-0} h2xx=${h2xx:-0} h429=${h429:-0}" >>"$log_path"
}

run_range() {
  local range="$1"
  local congress_start=${range%-*}
  local congress_end=${range#*-}
  if ! [[ "$congress_start" =~ ^[0-9]+$ && "$congress_end" =~ ^[0-9]+$ ]] || (( congress_start < 1 || congress_end < congress_start )); then
    echo "[$(date -u)] congress_per_key_ramp_invalid_range range=$range" >>"$log_path"
    return 2
  fi

  echo "[$(date -u)] congress_per_key_ramp_range_started start=$congress_start end=$congress_end" >>"$log_path"
  : >"$worker_log"
  setsid "$project_root/.venv/bin/python" scripts/run_congress_history.py \
    --categories bills amendments \
    --congress-start "$congress_start" --congress-end "$congress_end" \
    --mode backfill_descendants --max-pages 1000 --page-size 250 \
    --concurrency "$concurrency" --category-workers "$category_workers" \
    --key-wait-timeout 3600 --request-interval "$request_interval" \
    --active-key-budget "$active_key_budget" \
    >"$worker_log" 2>&1 &
  local worker_pid=$!
  local phase_status=0
  while bicam_worker_alive "$worker_pid"; do
    sleep 30
    local h429
    h429=$(bicam_db_count "select count(*) from crawl_run_fetches cf join crawl_runs cr on cr.id=cf.crawl_run_id where cr.started_at >= '${started_at}' and cf.api_key_name like 'congressional%' and cf.http_status=429;")
    h429=${h429:-0}
    if (( h429 >= max_new_429 )) || grep -Eq 'retry_after=([6-9][0-9]|[1-9][0-9]{2,})' "$worker_log" 2>/dev/null; then
      echo "[$(date -u)] congress_per_key_ramp_guard_stop range=$range h429=$h429" >>"$log_path"
      bicam_write_cooldown_marker "$worker_log"
      kill -TERM -- "-$worker_pid" 2>/dev/null || kill -TERM "$worker_pid" 2>/dev/null || true
      sleep 5
      kill -KILL -- "-$worker_pid" 2>/dev/null || true
      phase_status=42
      break
    fi
  done
  if (( phase_status == 0 )); then
    wait "$worker_pid" || phase_status=$?
  else
    wait "$worker_pid" 2>/dev/null || true
  fi
  write_checkpoint "congresses ${congress_start}-${congress_end}" "$phase_status"
  echo "[$(date -u)] congress_per_key_ramp_range_finished start=$congress_start end=$congress_end status=$phase_status" >>"$log_path"
  return "$phase_status"
}

run_refresh() {
  echo "[$(date -u)] congress_per_key_ramp_refresh_started batch_size=$batch_size" >>"$log_path"
  : >"$worker_log"
  setsid "$project_root/.venv/bin/python" scripts/run_congress_descendant_refresh.py \
    --categories amendments bills \
    --batch-size "$batch_size" --concurrency "$concurrency" \
    --page-size 250 --max-pages 1000 \
    --request-interval "$request_interval" --key-wait-timeout 3600 \
    --active-key-budget "$active_key_budget" \
    --max-batches "$max_batches" --checkpoint-path "$cursor_path" \
    >"$worker_log" 2>&1 &
  local worker_pid=$!
  local phase_status=0
  while bicam_worker_alive "$worker_pid"; do
    sleep 30
    local h429
    h429=$(bicam_db_count "select count(*) from crawl_run_fetches cf join crawl_runs cr on cr.id=cf.crawl_run_id where cr.started_at >= '${started_at}' and cf.api_key_name like 'congressional%' and cf.http_status=429;")
    h429=${h429:-0}
    if (( h429 >= max_new_429 )) || grep -Eq 'retry_after=([6-9][0-9]|[1-9][0-9]{2,})' "$worker_log" 2>/dev/null; then
      echo "[$(date -u)] congress_per_key_ramp_guard_stop refresh h429=$h429" >>"$log_path"
      bicam_write_cooldown_marker "$worker_log"
      kill -TERM -- "-$worker_pid" 2>/dev/null || kill -TERM "$worker_pid" 2>/dev/null || true
      sleep 5
      kill -KILL -- "-$worker_pid" 2>/dev/null || true
      phase_status=42
      break
    fi
  done
  if (( phase_status == 0 )); then
    wait "$worker_pid" || phase_status=$?
  else
    wait "$worker_pid" 2>/dev/null || true
  fi
  write_checkpoint "stored-root descendant refresh" "$phase_status"
  echo "[$(date -u)] congress_per_key_ramp_refresh_finished status=$phase_status" >>"$log_path"
  return "$phase_status"
}

ramp_status=0
if [[ "$ramp_mode" == "refresh" ]]; then
  run_refresh || ramp_status=$?
else
  IFS=',' read -r -a range_list <<<"$ranges"
  for range in "${range_list[@]}"; do
    range=${range//[[:space:]]/}
    run_range "$range" || { ramp_status=$?; break; }
  done
fi

if (( ramp_status == 0 )); then
  write_checkpoint "all configured ranges" "completed"
else
  write_checkpoint "ramp stopped before all configured ranges" "$ramp_status"
fi
echo "[$(date -u)] congress_per_key_ramp_finished status=$ramp_status elapsed_seconds=$(( $(date +%s) - started_epoch ))" >>"$log_path"
exit "$ramp_status"
