#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

# Historical Congress.gov partition queue (disabled; for supervised backfills).
# Blocks on the lock so two key pools never run at once.

lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
log_path="$reports_root/congress-partition-queue.log"
worker_log="$reports_root/congress-partition-worker.log"
checkpoint_prefix="$reports_root/congress-partition-checkpoint"
state_path=${BICAM_PARTITION_STATE_PATH:-$reports_root/congress-partition-queue-state.txt}
cooldown_path="$reports_root/congress-api-cooldown-until"

global_rate_limit=${BICAM_PARTITION_GLOBAL_RATE_LIMIT:-20000}
per_key_rate_limit=${BICAM_PARTITION_PER_KEY_RATE_LIMIT:-20000}
request_interval=${BICAM_PARTITION_REQUEST_INTERVAL:-0.10}
active_key_budget=${BICAM_PARTITION_ACTIVE_KEY_BUDGET:-34}
concurrency=${BICAM_PARTITION_CONCURRENCY:-34}
category_workers=${BICAM_PARTITION_CATEGORY_WORKERS:-4}
max_pages=${BICAM_PARTITION_MAX_PAGES:-1000}
page_size=${BICAM_PARTITION_PAGE_SIZE:-250}
max_new_429=${BICAM_PARTITION_MAX_NEW_429:-3}
group_size=${BICAM_PARTITION_GROUP_SIZE:-10}
between_group_sleep=${BICAM_PARTITION_BETWEEN_GROUP_SECONDS:-5}
checkpoint_interval=${BICAM_PARTITION_CHECKPOINT_INTERVAL:-300}

mkdir -p "$reports_root"

if bicam_cooldown_active; then
  bicam_wait_for_cooldown congress_partition_queue_cooldown_sleep
fi

exec 9>"$lock_path"

cd "$project_root" || exit 1
bicam_load_project_env

started_at=$(date -u '+%Y-%m-%d %H:%M:%S%z')
started_epoch=$(date +%s)
echo "[$(date -u)] congress_partition_queue_started group_size=$group_size request_interval=$request_interval" >>"$log_path"

export BICAM_CONGRESS_GLOBAL_RATE_LIMIT="$global_rate_limit"
export BICAM_CONGRESS_PER_KEY_RATE_LIMIT="$per_key_rate_limit"
export BICAM_CONGRESS_KEY_SLOTS="${BICAM_CONGRESS_KEY_SLOTS:-2}"

write_checkpoint() {
  local label="$1"
  local status="$2"
  local stamp
  stamp=$(date -u '+%Y%m%dT%H%M%SZ')
  local attempts h2xx h429
  # Filter via crawl_runs to use the crawl_run_fetches(crawl_run_id) index.
  attempts=$(bicam_db_count "select coalesce(sum(coalesce(cf.attempt_count,0)),0) from crawl_run_fetches cf join crawl_runs cr on cr.id=cf.crawl_run_id where cr.started_at >= '${started_at}' and cf.api_key_name like 'congressional%';")
  h2xx=$(bicam_db_count "select count(*) from crawl_run_fetches cf join crawl_runs cr on cr.id=cf.crawl_run_id where cr.started_at >= '${started_at}' and cf.api_key_name like 'congressional%' and cf.http_status between 200 and 299;")
  h429=$(bicam_db_count "select count(*) from crawl_run_fetches cf join crawl_runs cr on cr.id=cf.crawl_run_id where cr.started_at >= '${started_at}' and cf.api_key_name like 'congressional%' and cf.http_status=429;")
  {
    echo "# Congress historical partition checkpoint"
    echo
    echo "Checkpoint: $(date -u '+%Y-%m-%d %H:%M UTC')"
    echo
    echo "- phase: $label"
    echo "- status: $status"
    echo "- configured Congress scope: 1–119 (1789–present)"
    echo "- aggregate local pressure guard/pacing setting: $global_rate_limit attempts/hour (not an account quota)"
    echo "- per-credential local window: $per_key_rate_limit attempts/hour (shared across virtual slots)"
    echo "- process pacer: ${request_interval}s"
    echo "- attempts since queue start: ${attempts:-0}"
    echo "- successful HTTP responses: ${h2xx:-0}"
    echo "- HTTP 429 responses: ${h429:-0}"
    echo "- queue start: $started_at"
    echo
    echo "The raw archive and crawl ledger remain authoritative; this checkpoint is resumable and contains no credentials."
  } >"$checkpoint_prefix-$stamp.md"
  echo "[$(date -u)] congress_partition_checkpoint phase=$label status=$status attempts=${attempts:-0} h2xx=${h2xx:-0} h429=${h429:-0}" >>"$log_path"
}

write_queue_state() {
  local next_start="$1"
  local last_completed_end="$2"
  local state="$3"
  local temporary="${state_path}.tmp"
  {
    printf 'next_start=%s\n' "$next_start"
    printf 'last_completed_end=%s\n' "$last_completed_end"
    printf 'state=%s\n' "$state"
    printf 'updated_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  } >"$temporary"
  mv "$temporary" "$state_path"
}

run_group() {
  local group_start="$1"
  local group_end="$2"
  # Hold the lock per Congress group so the descendant refresher can run between groups.
  if bicam_cooldown_active; then
    echo "[$(date -u)] congress_partition_queue_cooldown_wait" >>"$log_path"
    return 42
  fi
  if [[ "${BICAM_PARTITION_GROUP_LOCK_HELD:-0}" != "1" ]]; then
    (
      flock -w "${BICAM_PARTITION_LOCK_WAIT_SECONDS:-1800}" 9 || exit $?
      export BICAM_PARTITION_GROUP_LOCK_HELD=1
      exec "$0" --run-group "$group_start" "$group_end"
    )
    return $?
  fi
  if bicam_cooldown_active; then
    echo "[$(date -u)] congress_partition_queue_cooldown_wait" >>"$log_path"
    return 42
  fi
  echo "[$(date -u)] congress_partition_lock_acquired start=$group_start end=$group_end" >>"$log_path"
  echo "[$(date -u)] congress_partition_group_started start=$group_start end=$group_end" >>"$log_path"
  : >"$worker_log"
  setsid "$project_root/.venv/bin/python" scripts/run_congress_history.py \
    --categories amendments bills committee-meetings committee-prints committee-reports committees hearings nominations treaties \
    --congress-start "$group_start" --congress-end "$group_end" \
    --mode backfill_descendants --max-pages "$max_pages" --page-size "$page_size" \
    --concurrency "$concurrency" --category-workers "$category_workers" \
    --key-wait-timeout 3600 --request-interval "$request_interval" \
    --active-key-budget "$active_key_budget" \
    >"$worker_log" 2>&1 &
  local worker_pid=$!
  local phase_status=0
  local next_checkpoint_epoch=$(( $(date +%s) + checkpoint_interval ))
  while bicam_worker_alive "$worker_pid"; do
    sleep 30
    if (( checkpoint_interval > 0 && $(date +%s) >= next_checkpoint_epoch )); then
      write_checkpoint "Congress ${group_start}-${group_end} in progress" "running"
      next_checkpoint_epoch=$(( $(date +%s) + checkpoint_interval ))
    fi
    local h429
    h429=$(bicam_db_count "select count(*) from crawl_run_fetches cf join crawl_runs cr on cr.id=cf.crawl_run_id where cr.started_at >= '${started_at}' and cf.api_key_name like 'congressional%' and cf.http_status=429;")
    h429=${h429:-0}
    if (( h429 >= max_new_429 )) || grep -Eq 'retry_after=([6-9][0-9]|[1-9][0-9]{2,})' "$worker_log" 2>/dev/null; then
      echo "[$(date -u)] congress_partition_guard_stop start=$group_start end=$group_end h429=$h429" >>"$log_path"
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
  write_checkpoint "Congress ${group_start}-${group_end}" "$phase_status"
  echo "[$(date -u)] congress_partition_group_finished start=$group_start end=$group_end status=$phase_status" >>"$log_path"
  return "$phase_status"
}

if [[ "${1:-}" == "--run-group" ]]; then
  run_group "${2:?missing group start}" "${3:?missing group end}"
  exit $?
fi

next_start=$(sed -n 's/^next_start=//p' "$state_path" 2>/dev/null | head -1 || true)
queue_state=$(sed -n 's/^state=//p' "$state_path" 2>/dev/null | head -1 || true)
if [[ "$next_start" =~ ^[0-9]+$ ]] && (( next_start > 119 )) && [[ "$queue_state" == "completed" ]]; then
  write_checkpoint "all Congress partitions" "completed"
  echo "[$(date -u)] congress_partition_queue_already_complete next_start=$next_start" >>"$log_path"
  exit 0
fi
if ! [[ "$next_start" =~ ^[0-9]+$ ]] || (( next_start < 1 || next_start > 119 )); then
  next_start=1
fi
echo "[$(date -u)] congress_partition_queue_resume next_start=$next_start state_path=$state_path" >>"$log_path"
# Mark the cursor running while a group is in progress.
write_queue_state "$next_start" "$((next_start - 1))" running

queue_status=0
for ((group_start=next_start; group_start<=119; group_start+=group_size)); do
  group_end=$((group_start + group_size - 1))
  (( group_end > 119 )) && group_end=119
  if run_group "$group_start" "$group_end"; then
    write_queue_state "$((group_end + 1))" "$group_end" completed
  else
    queue_status=$?
    write_queue_state "$group_start" "$((group_start - 1))" paused_after_group_failure
    break
  fi
  # Pause between groups so the other Congress lane can take the lock.
  if (( between_group_sleep > 0 )); then
    echo "[$(date -u)] congress_partition_between_group_sleep seconds=$between_group_sleep" >>"$log_path"
    sleep "$between_group_sleep"
  fi
done

write_checkpoint "all Congress partitions" "$queue_status"
echo "[$(date -u)] congress_partition_queue_finished status=$queue_status elapsed_seconds=$(( $(date +%s) - started_epoch ))" >>"$log_path"
exit "$queue_status"
