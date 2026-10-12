#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

log_file="$reports_root/congress-list-page-repair.log"
lock_file=/run/user/1000/bicam-ingestion.lock

mkdir -p "$reports_root"
cd "$project_root" || exit 1
bicam_load_project_env

# Blocks on the lock: may start at boot while a crawl or dbt build is running.
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  exec 9>"$lock_file"
  (
    flock 9 || exit $?
    export BICAM_INGESTION_LOCK_HELD=1
    exec "$0" "$@"
  )
  exit $?
fi

# The amendment reserve lane has its own budget, so don't wait for it to finish.
while :; do
  active_runs=$(bicam_psql -At -c \
    "select count(*)
       from crawl_runs
      where status = 'running'
        and not (category = 'amendments' and mode = 'backfill_refresh_descendants');" \
    2>/dev/null || echo 1)
  if [ "$active_runs" = "0" ]; then
    break
  fi
  sleep 60
done

{
  date -u
  echo "list_page_repair_started"
  export BICAM_CONGRESS_GLOBAL_RATE_LIMIT="${BICAM_LIST_PAGE_REPAIR_GLOBAL_RATE_LIMIT:-16000}"
  export BICAM_CONGRESS_PER_KEY_RATE_LIMIT="${BICAM_LIST_PAGE_REPAIR_PER_KEY_RATE_LIMIT:-20000}"
  "$project_root/.venv/bin/python" scripts/recover_congress_list_pages.py \
    --category bills \
    --congress-start 83 \
    --congress-end 92 \
    --start-offset 250 \
    --end-offset 500 \
    --chunk-size 10 \
    --min-chunk-size 1 \
    --request-interval "${BICAM_LIST_PAGE_REPAIR_REQUEST_INTERVAL:-0.24}" \
    --key-wait-timeout 3600 \
    --active-key-budget 32 \
    --max-attempts "${BICAM_LIST_PAGE_REPAIR_MAX_ATTEMPTS:-5000}"
  rc=$?
  echo "list_page_repair_rc=$rc"
  exit "$rc"
} >>"$log_file" 2>&1
