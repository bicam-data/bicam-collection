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

# Markdown section with the last lines of a log, for checkpoint files.
log_tail() {
  local title=$1 log=$2 lines=$3
  printf '\n## %s\n```text\n' "$title"
  tail -n "$lines" "$log" 2>/dev/null || true
  printf '```\n'
}
