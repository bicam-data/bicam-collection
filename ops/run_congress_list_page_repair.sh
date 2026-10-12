#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

log_file="$reports_root/congress-list-page-repair.log"
lock_file=/run/user/1000/bicam-ingestion.lock

mkdir -p "$reports_root"
cd "$project_root" || exit 1
bicam_load_project_env
bicam_prefer_container_db_password

# Serialize with the main supervisor and its post-crawl/dbt phase.  The
# blocking flock is intentional: this service may start at boot while a
# historical crawl is already active, but must not consume the shared API
# budget or race the reconciliation build.
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  exec 9>"$lock_file"
  (
    flock 9 || exit $?
    export BICAM_INGESTION_LOCK_HELD=1
    exec "$0" "$@"
  )
  exit $?
fi

# The post-crawl supervisor deliberately allows the dedicated amendment
# reserve to finish in parallel with its dbt phase. It owns a separate 4k/h
# source-budget lane, so this repair can start as soon as all other Congress
# crawl runs finish and use its complementary 16k/h budget. Waiting for the
# reserve as well would leave the source quota idle without improving safety.
while :; do
  active_runs=$(docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -c \
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
