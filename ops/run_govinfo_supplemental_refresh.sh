#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

report_root="$reports_root"
log_path="$report_root/govinfo-supplemental-refresh.log"
lock_path="/run/user/$(id -u)/bicam-ingestion.lock"

mkdir -p "$report_root"
exec >>"$log_path" 2>&1

# Hold the lock so dbt never builds from a half-updated crawl.
lock_wait_seconds=${BICAM_GOVINFO_SUPPLEMENTAL_LOCK_WAIT_SECONDS:-1800}
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) govinfo_supplemental_refresh lock_busy"
  exit 0
fi

cd "$project_root" || exit 1
bicam_load_project_env
export DBT_PROFILES_DIR="$project_root/dbt_project"

started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
window_end=$(date -u +%Y-%m-%dT%H:%M:%SZ)
window_start=$(date -u -d "${BICAM_GOVINFO_DIRECTORY_OVERLAP_DAYS:-14} days ago" +%Y-%m-%dT%H:%M:%SZ)
current_congress=""
status=0
directory_status=0
treaty_status=0
dbt_status=0
test_status=0
validation_status=0

current_congress="$(uv run python scripts/resolve_current_congress.py)" || status=$?

# CDIR is small and date-indexed. A bounded overlap captures corrected editions
# without replaying the historical directory collection.
if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_DIRECTORY_TIMEOUT_SECONDS:-900}s" \
    uv run python scripts/run_crawl.py \
      --category govinfo-congressional-directories \
      --mode full \
      --from-datetime "$window_start" \
      --to-datetime "$window_end" \
      --page-size "${BICAM_GOVINFO_SUPPLEMENTAL_PAGE_SIZE:-100}" \
      --async \
      --concurrency "${BICAM_GOVINFO_SUPPLEMENTAL_CONCURRENCY:-3}" \
      --request-interval "${BICAM_GOVINFO_SUPPLEMENTAL_REQUEST_INTERVAL:-0.25}" \
      --key-wait-timeout "${BICAM_GOVINFO_SUPPLEMENTAL_KEY_WAIT_TIMEOUT:-600}" \
      || directory_status=$?
  if (( directory_status != 0 )); then status=1; fi
fi

# The CDOC collection is broad, so refresh only deterministic treaty package
# IDs for the current Congress. This also discovers newly indexed treaties.
if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_TREATY_TIMEOUT_SECONDS:-900}s" \
    uv run python scripts/backfill_govinfo_treaty_documents.py \
      --congress "$current_congress" \
      --limit "${BICAM_GOVINFO_TREATY_LIMIT:-500}" \
      --refresh \
      --concurrency "${BICAM_GOVINFO_SUPPLEMENTAL_CONCURRENCY:-3}" \
      --request-interval "${BICAM_GOVINFO_SUPPLEMENTAL_REQUEST_INTERVAL:-0.25}" \
      --key-wait-timeout "${BICAM_GOVINFO_SUPPLEMENTAL_KEY_WAIT_TIMEOUT:-600}" \
      || treaty_status=$?
  if (( treaty_status != 0 )); then status=1; fi
fi

if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_SUPPLEMENTAL_DBT_TIMEOUT_SECONDS:-900}s" \
    uv run dbt run --no-partial-parse --project-dir dbt_project --profiles-dir dbt_project \
      --select int_raw_response_versions int_raw_responses__current \
        stg_govinfo__congressional_directories \
        stg_govinfo__congressional_directory_members \
        stg_govinfo__treaty_documents_packages \
        stg_govinfo__treaty_documents_granules \
        congresses_directories members_metadata members_terms \
        treaties_metadata treaties_metadata_granules \
      || dbt_status=$?
  if (( dbt_status != 0 )); then status=1; fi
fi

if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_SUPPLEMENTAL_TEST_TIMEOUT_SECONDS:-300}s" \
    uv run dbt test --no-partial-parse --project-dir dbt_project --profiles-dir dbt_project \
      --select stg_govinfo__treaty_documents_packages \
        stg_govinfo__treaty_documents_granules \
        congresses_directories members_metadata members_terms \
        members_terms_preserve_directory_fields \
        treaties_metadata treaties_metadata_granules \
      || test_status=$?
  if (( test_status != 0 )); then status=1; fi
fi

if (( status == 0 )); then
  uv run python scripts/validate_bicam_product.py >/dev/null || validation_status=$?
  if (( validation_status != 0 )); then status=1; fi
fi

checkpoint_timestamp=$(date -u +%Y%m%dT%H%M%SZ)
checkpoint_path="$report_root/govinfo-supplemental-checkpoint-${checkpoint_timestamp}.md"
{
  echo "# GovInfo supplemental refresh checkpoint"
  echo
  echo "Generated: $(date -u '+%Y-%m-%d %H:%M UTC')"
  echo
  echo "- Overall status: ${status}"
  echo "- Current Congress: ${current_congress:-unresolved}"
  echo "- Directory window: ${window_start} through ${window_end}"
  echo "- Directory crawl status: ${directory_status}"
  echo "- Treaty-document refresh status: ${treaty_status}"
  echo "- dbt refresh status: ${dbt_status}"
  echo "- dbt test status: ${test_status}"
  echo "- Public-product validation status: ${validation_status}"
  echo
  echo '| Public table | Rows |'
  echo '|---|---:|'
  bicam_psql -At -F '|' -c \
     "select 'congresses_directories', count(*) from bicam.congresses_directories
     union all select 'members_metadata', count(*) from bicam.members_metadata
     union all select 'members_terms', count(*) from bicam.members_terms
     union all select 'treaties_metadata', count(*) from bicam.treaties_metadata
     union all select 'treaties_metadata_granules', count(*) from bicam.treaties_metadata_granules
     order by 1" 2>/dev/null | while IFS='|' read -r table_name row_count; do
       [ -n "$table_name" ] || continue
       echo "| ${table_name} | ${row_count} |"
     done
} >"$checkpoint_path"
ln -sfn "$(basename "$checkpoint_path")" "$report_root/govinfo-supplemental-checkpoint-latest.md"

echo "${started_at} govinfo_supplemental_refresh_started"
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) govinfo_supplemental_refresh_finished status=${status} checkpoint=${checkpoint_path}"
exit "$status"
