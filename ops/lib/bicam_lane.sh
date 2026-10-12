#!/usr/bin/env bash
# Shared preamble for the timer-driven lane wrappers in ../.  Sourced, never
# executed.  Each function is the block that used to be pasted into every
# wrapper; the wrappers still own their lock timeouts, lock-busy handling,
# exit codes and log lines.

# Paths are derived from this file's location so the repo can live anywhere.
# Reports hold checkpoints, logs and exports (hundreds of GB); they sit
# outside the repo and default to a sibling of it.
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
reports_root="${BICAM_REPORTS_DIR:-$(dirname "$project_root")/reports}"

# The crawler reads the project dotenv itself; dbt and psql do not.  Callers
# cd into bicam-collection first.  The path is deliberately relative so a
# wrapper that forgot to cd fails here instead of against the wrong database.
bicam_load_project_env() {
  set -a
  . ./.env
  set +a
}

# The database container can outlive the .env that created it.  Prefer its
# live credential so a successful crawl is not followed by a dbt/psql
# authentication failure.
bicam_prefer_container_db_password() {
  if [ -z "${BICAM_DB_PASSWORD_OVERRIDE:-}" ]; then
    BICAM_DB_PASSWORD_OVERRIDE="$(docker inspect bicam-collection-postgres -f '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null | awk -F= '$1=="POSTGRES_PASSWORD"{print substr($0,index($0,"=")+1)}')"
    export BICAM_DB_PASSWORD_OVERRIDE
  fi
  if [ -n "${BICAM_DB_PASSWORD_OVERRIDE:-}" ]; then
    export DB_PASSWORD="$BICAM_DB_PASSWORD_OVERRIDE"
  fi
}

# Re-run the calling wrapper with the shared ingestion lock held on fd 9.
#
#   if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
#     bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
#     ...record this wrapper's lock-busy outcome and exit...
#   fi
#
# Words between the lock path and `--` go to flock verbatim (-n, or -w
# SECONDS); words after `--` are the wrapper's own "$@".  The child inherits
# fd 9 and BICAM_INGESTION_LOCK_HELD=1, so it skips the guard.  When the child
# finishes this process exits 0; when flock fails for any reason other than a
# busy or timed-out lock (75) this process exits with flock's status.  Only
# the busy case returns, so each wrapper keeps its own checkpoint text and
# exit code for that path.  Call it from the top level of the wrapper, not
# from a subshell, or the exits above would not end the wrapper.
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
  if (( lock_status == 0 )); then
    exit 0
  else
    if (( lock_status != 75 )); then
      exit "$lock_status"
    fi
  fi
}

# Congress.gov cooldown coordination shared by the persistent per-key ramp and
# partition-queue lanes.  These read the caller's $cooldown_path and $log_path.
bicam_cooldown_active() {
  local until now
  until=$(cat "$cooldown_path" 2>/dev/null || true)
  now=$(date +%s)
  [[ "$until" =~ ^[0-9]+$ ]] && (( until > now ))
}

# Sleep through a server cooldown in short chunks instead of exiting into a
# rapid systemd Restart=always loop.  Re-reading the marker lets an operator
# shorten or clear the cooldown without waiting the full server interval.
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
  # Re-arm the one-shot resume timer so a future coordinated window remains
  # self-healing even after the currently armed timer has already fired.
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

bicam_db_count() {
  local sql="$1"
  docker exec -e PGPASSWORD="${DB_PASSWORD:-}" bicam-collection-postgres \
    psql -U "${DB_USER:-admin}" -d "${DB_NAME:-bicam_collection}" -At -c "$sql" 2>/dev/null \
    | tr -d '[:space:]'
}
