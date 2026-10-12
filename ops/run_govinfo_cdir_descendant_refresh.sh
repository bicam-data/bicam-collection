#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

# Historical CDIR package discovery and descendant traversal are separate
# operations.  The daily date-window job finds newly revised packages, while
# this cursor lane walks every stored package detail and fills its granule
# list/detail descendants in bounded slices.
report_root="$reports_root"
log_path="$report_root/govinfo-cdir-descendant-refresh.log"
lock_path="/run/user/$(id -u)/bicam-ingestion.lock"

mkdir -p "$report_root"
exec >>"$log_path" 2>&1

lock_wait_seconds=${BICAM_GOVINFO_CDIR_LOCK_WAIT_SECONDS:-1800}
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  checkpoint="$report_root/govinfo-cdir-descendant-checkpoint-${timestamp}.md"
  {
    echo "# GovInfo Congressional Directory descendant refresh"
    echo
    echo "Generated: $(date -u '+%Y-%m-%d %H:%M UTC')"
    echo
    echo "- Overall status: lock_busy"
    echo "- Note: another ingestion lane owns the shared lock; the next scheduled run will retry."
  } >"$checkpoint"
  ln -sfn "$(basename "$checkpoint")" "$report_root/govinfo-cdir-descendant-checkpoint-latest.md"
  exit 0
fi

cd "$project_root" || exit 1
bicam_load_project_env
export DBT_PROFILES_DIR="$project_root/dbt_project"

batch_size=${BICAM_GOVINFO_CDIR_BATCH_SIZE:-12}
# Twenty-four bounded rounds cover the current 239 stored packages in one
# service invocation while retaining a small per-round root batch.
max_batches=${BICAM_GOVINFO_CDIR_MAX_BATCHES:-24}
concurrency=${BICAM_GOVINFO_CDIR_CONCURRENCY:-4}
page_size=${BICAM_GOVINFO_CDIR_PAGE_SIZE:-100}
max_pages=${BICAM_GOVINFO_CDIR_MAX_PAGES:-1000}
request_interval=${BICAM_GOVINFO_CDIR_REQUEST_INTERVAL:-0.25}
key_wait_timeout=${BICAM_GOVINFO_CDIR_KEY_WAIT_TIMEOUT:-1200}
active_key_budget=${BICAM_GOVINFO_CDIR_ACTIVE_KEY_BUDGET:-8}
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
status=0
dbt_status=0

uv run python scripts/run_congress_descendant_refresh.py \
  --source govinfo \
  --categories govinfo-congressional-directories \
  --batch-size "$batch_size" \
  --concurrency "$concurrency" \
  --page-size "$page_size" \
  --max-pages "$max_pages" \
  --max-batches "$max_batches" \
  --request-interval "$request_interval" \
  --key-wait-timeout "$key_wait_timeout" \
  --active-key-budget "$active_key_budget" \
  --checkpoint-path "$report_root/govinfo-cdir-descendant-checkpoint.json" \
  || status=$?

# Rebuild only the CDIR-backed public surfaces after a successful crawl.  The
# models are selected explicitly so this lane cannot trigger a full product
# rebuild while the historical cursor is advancing.
if (( status == 0 )); then
  uv run dbt run --no-partial-parse --project-dir dbt_project --profiles-dir dbt_project \
    --select int_raw_response_versions int_raw_responses__current \
      int_govinfo_member_references \
      bridge_govinfo__member_references \
      stg_govinfo__congressional_directories \
      stg_govinfo__congressional_directory_members \
      congresses_directories \
      congresses_directories_isbn \
      members_metadata \
      members_terms \
    || dbt_status=$?
  if (( dbt_status == 0 )); then
    uv run dbt test --no-partial-parse --project-dir dbt_project --profiles-dir dbt_project \
      --select members_metadata_preserves_cdir_granules \
        congresses_directories_isbn_preserve_package_observations \
      || dbt_status=$?
  fi
  if (( dbt_status != 0 )); then status=1; fi
fi

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
checkpoint="$report_root/govinfo-cdir-descendant-refresh-${timestamp}.md"
{
  echo "# GovInfo Congressional Directory descendant refresh"
  echo
  echo "Generated: $(date -u '+%Y-%m-%d %H:%M UTC')"
  echo
  echo "- Overall status: ${status}"
  echo "- Descendant cursor status: $(if [ -f "$report_root/govinfo-cdir-descendant-checkpoint.json" ]; then sed -n '1,120p' "$report_root/govinfo-cdir-descendant-checkpoint.json"; else echo unavailable; fi)"
  echo "- dbt refresh status: ${dbt_status}"
  echo
  echo '| Coverage measure | Count |'
  echo '|---|---:|'
  bicam_psql -At -F '|' -c "
    select 'CDIR package details', count(*) from raw_responses
      where source='govinfo' and category='govinfo-congressional-directories'
        and node_type='govinfo_congressional_directory_package_detail'
    union all select 'CDIR granule-list pages', count(*) from raw_responses
      where source='govinfo' and category='govinfo-congressional-directories'
        and node_type='govinfo_congressional_directory_granule_list'
    union all select 'CDIR granule details', count(*) from raw_responses
      where source='govinfo' and category='govinfo-congressional-directories'
        and node_type='govinfo_congressional_directory_granule_detail'
    union all select 'CDIR member-class granules', count(*) from raw_responses
      where source='govinfo' and category='govinfo-congressional-directories'
        and node_type='govinfo_congressional_directory_granule_detail'
        and upper(coalesce(response->>'granuleClass',''))='CONGRESSMEMBERSTATE'
    union all select 'canonical members', count(*) from bicam.members
    union all select 'member metadata observations', count(*) from bicam.members_metadata
    union all select 'resolved member metadata observations', count(*) from bicam.members_metadata
      where bioguide_id is not null
  " 2>/dev/null | while IFS='|' read -r measure count; do
      [ -n "$measure" ] || continue
      echo "| ${measure} | ${count} |"
    done
} >"$checkpoint"
ln -sfn "$(basename "$checkpoint")" "$report_root/govinfo-cdir-descendant-refresh-latest.md"
echo "${started_at} govinfo_cdir_descendant_refresh_started"
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) govinfo_cdir_descendant_refresh_finished status=${status} checkpoint=${checkpoint}"
exit "$status"
