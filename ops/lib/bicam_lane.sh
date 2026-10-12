#!/usr/bin/env bash
# Shared helpers for the lane wrappers in ../; sourced, never executed.

# Reports (checkpoints, logs, exports; hundreds of GB) live outside the repo.
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
reports_root="${BICAM_REPORTS_DIR:-$(dirname "$project_root")/reports}"

# The crawler reads the project dotenv itself; dbt and psql do not.
bicam_load_project_env() {
  set -a
  . "$project_root/.env"
  set +a
}

# psql against the database configured in .env, with the same DB_* defaults
# as config.py and profiles.yml. Requires bicam_load_project_env first.
bicam_psql() {
  PGHOST="${DB_HOST:-localhost}" \
  PGPORT="${DB_PORT:-5433}" \
  PGUSER="${DB_USER:-admin}" \
  PGPASSWORD="${DB_PASSWORD:-}" \
  PGDATABASE="${DB_NAME:-bicam_collection}" \
    psql "$@"
}

# Re-run the calling wrapper with the shared ingestion lock held on fd 9:
#
#   bicam_reexec_under_ingestion_lock "$lock_path" -w 30 -- "$@"
#
# Arguments before `--` go to flock. The re-run child sees
# BICAM_INGESTION_LOCK_HELD=1. Exits when the child finishes or flock fails;
# returns only when the lock is busy, so the caller handles that case. Call
# from the wrapper's top level, not a subshell.
bicam_reexec_under_ingestion_lock() {
  local lock_path=$1
  shift
  local flock_args=()
  while [[ $1 != -- ]]; do
    flock_args+=("$1")
    shift
  done
  shift
  exec 9>"$lock_path"
  (
    flock "${flock_args[@]}" -E 75 9 || exit $?
    export BICAM_INGESTION_LOCK_HELD=1
    exec "$0" "$@"
  )
  lock_status=$?
  if (( lock_status != 75 )); then
    exit "$lock_status"
  fi
}

# Congress.gov Retry-After coordination; uses the caller's $cooldown_path and $log_path.
bicam_cooldown_active() {
  local until now
  until=$(cat "$cooldown_path" 2>/dev/null || true)
  now=$(date +%s)
  [[ "$until" =~ ^[0-9]+$ ]] && (( until > now ))
}

# Sleep through the cooldown in chunks of at most a minute (rather than exit
# into a Restart=always loop), re-reading the marker so it can be cleared early.
bicam_wait_for_cooldown() {
  local log_label=$1
  local until now delay
  until=$(cat "$cooldown_path" 2>/dev/null || true)
  now=$(date +%s)
  if [[ "$until" =~ ^[0-9]+$ ]] && (( until > now )); then
    delay=$((until - now))
    echo "[$(date -u)] $log_label seconds=$delay" >>"$log_path"
    while bicam_cooldown_active; do
      sleep "$((delay > 60 ? 60 : delay))"
      until=$(cat "$cooldown_path" 2>/dev/null || true)
      now=$(date +%s)
      if [[ "$until" =~ ^[0-9]+$ ]] && (( until > now )); then
        delay=$((until - now))
      else
        break
      fi
    done
  fi
}

bicam_write_cooldown_marker() {
  local source_log="$1"
  local retry_after until temporary
  retry_after=$(grep -Eo 'retry_after=[0-9]+([.][0-9]+)?' "$source_log" 2>/dev/null \
    | awk -F= 'BEGIN { max=0 } { if ($2 > max) max=$2 } END { if (max > 0) printf "%.0f", max + 30; else print 300 }')
  retry_after=${retry_after:-300}
  until=$(( $(date +%s) + retry_after ))
  temporary="${cooldown_path}.tmp"
  printf '%s\n' "$until" >"$temporary"
  mv "$temporary" "$cooldown_path"
  echo "[$(date -u)] congress_api_cooldown_marker until=$until retry_after=$retry_after" >>"$log_path"
  # Re-arm the one-shot resume timer, which may already have fired.
  systemctl --user restart bicam-congress-cooldown-resume.timer >/dev/null 2>&1 || true
}

# An exited-but-unreaped worker still answers kill -0; treat zombies as dead.
bicam_worker_alive() {
  local pid="$1"
  if ! kill -0 "$pid" 2>/dev/null; then
    return 1
  fi
  local state
  state=$(ps -o stat= -p "$pid" 2>/dev/null | tr -d '[:space:]')
  [[ -n "$state" && "$state" != Z* ]]
}

# Stop backends left behind by a timed-out dbt step, matched by its unique
# application name: cancel twice (the first can race the client disconnect),
# then terminate any stuck in I/O.
bicam_stop_dbt_backends() {
  local app_name=$1 action
  for action in cancel cancel terminate; do
    bicam_psql -At -v app_name="$app_name" \
      <<< "select pg_${action}_backend(pid) from pg_stat_activity where application_name = :'app_name' and pid <> pg_backend_pid();" >/dev/null 2>&1 || true
    [[ $action == terminate ]] || sleep 2
  done
}

bicam_db_count() {
  local sql="$1"
  bicam_psql -At -c "$sql" 2>/dev/null | tr -d '[:space:]'
}

# Markdown section with the last lines of a log, for checkpoint files.
log_tail() {
  local title=$1 log=$2 lines=$3
  printf '\n## %s\n```text\n' "$title"
  tail -n "$lines" "$log" 2>/dev/null || true
  printf '```\n'
}
