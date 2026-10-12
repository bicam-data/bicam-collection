#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

log_path="$reports_root/govinfo-incremental-windows.log"
lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
latest_checkpoint="$reports_root/govinfo-window-checkpoint-latest.md"
attempt_checkpoint="$reports_root/govinfo-window-checkpoint-attempt-latest.md"

mkdir -p "$reports_root"
exec >>"$log_path" 2>&1

# Wait behind other lanes rather than skip: an hourly skip can keep lining up
# with the next trigger and stall the GovInfo frontier.
lock_wait_seconds=${BICAM_GOVINFO_LOCK_WAIT_SECONDS:-1800}
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
  checkpoint_timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  checkpoint_path="$reports_root/govinfo-window-checkpoint-${checkpoint_timestamp}.md"
  {
    echo "# GovInfo incremental-window checkpoint"
    echo
    echo "Generated: $(date -u '+%Y-%m-%d %H:%M UTC')"
    echo
    echo "Runner exit status: 0"
    echo "Status: lock_busy"
    echo "Another ingestion/reconciliation lane held the shared lock for ${lock_wait_seconds}s; the next scheduled trigger will retry."
  } >"$checkpoint_path"
  # A lock-busy skip is recorded only under the attempt pointer.
  ln -sfn "$(basename "$checkpoint_path")" "$attempt_checkpoint"
  echo "govinfo_incremental_windows_lock_busy=1"
  exit 0
fi

started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "$started_at govinfo_incremental_windows_started"
cd "$project_root" || exit 1

bicam_load_project_env
export DBT_PROFILES_DIR="$project_root/dbt_project"

dbt_app_name="bicam-govinfo-dbt-$(date -u '+%Y%m%dT%H%M%SZ')-$$"

cleanup_dbt_backends() {
  local exit_status=$?
  bicam_stop_dbt_backends "$dbt_app_name"
  return "$exit_status"
}

trap cleanup_dbt_backends EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

status=0
timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_WINDOW_TIMEOUT_SECONDS:-3600}s" \
  uv run python scripts/run_govinfo_incremental_windows.py \
    --window-hours "${BICAM_GOVINFO_WINDOW_HOURS:-6}" \
    --lag-minutes "${BICAM_GOVINFO_WINDOW_LAG_MINUTES:-5}" \
    --minimum-window-minutes "${BICAM_GOVINFO_WINDOW_MINIMUM_MINUTES:-5}" \
    --max-windows "${BICAM_GOVINFO_WINDOW_MAX_WINDOWS:-4}" \
    --page-size "${BICAM_GOVINFO_WINDOW_PAGE_SIZE:-100}" \
    --concurrency "${BICAM_GOVINFO_WINDOW_CONCURRENCY:-3}" \
    --request-interval "${BICAM_GOVINFO_WINDOW_REQUEST_INTERVAL:-0.25}" \
    --key-wait-timeout "${BICAM_GOVINFO_WINDOW_KEY_WAIT_TIMEOUT:-600}" || status=$?

# Refresh the GovInfo bill identity model so the probes see this window's packages.
bill_identity_status=0
if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_BILL_IDENTITY_TIMEOUT_SECONDS:-120}s" \
    uv run dbt run --no-partial-parse --project-dir dbt_project \
      --profiles-dir dbt_project --select int_bills__govinfo || bill_identity_status=$?
  if (( bill_identity_status != 0 )); then
    status=1
  fi
fi

# Upper bound for the hearing probe: the newest Congress the Congress lane has seen.
current_congress_status=0
current_congress=""
if (( status == 0 )); then
  if [[ -n "${BICAM_CURRENT_CONGRESS:-}" ]]; then
    current_congress="$({ uv run python scripts/resolve_current_congress.py --override "$BICAM_CURRENT_CONGRESS"; })" \
      || current_congress_status=$?
  else
    current_congress="$({ uv run python scripts/resolve_current_congress.py; })" \
      || current_congress_status=$?
  fi
  if (( current_congress_status != 0 )); then
    status=1
  fi
fi

# Probe Congress.gov for GovInfo committee reports missing a counterpart.
congress_report_probe_status=0
if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_CONGRESS_REPORT_PROBE_TIMEOUT_SECONDS:-300}s" \
    .venv/bin/python scripts/backfill_congress_committee_reports.py \
      --limit "${BICAM_GOVINFO_CONGRESS_REPORT_PROBE_LIMIT:-100}" \
      --concurrency "${BICAM_GOVINFO_CONGRESS_REPORT_PROBE_CONCURRENCY:-1}" \
      --request-interval "${BICAM_GOVINFO_CONGRESS_REPORT_PROBE_INTERVAL:-1.0}" \
      --key-wait-timeout "${BICAM_GOVINFO_CONGRESS_REPORT_PROBE_KEY_WAIT_TIMEOUT:-600}" \
      --active-key-budget "${BICAM_GOVINFO_CONGRESS_REPORT_PROBE_ACTIVE_KEY_BUDGET:-1}" \
      || congress_report_probe_status=$?
  if (( congress_report_probe_status != 0 )); then
    status=1
  fi
fi
echo "congress_committee_report_probe_status=$congress_report_probe_status"

# Rebuild the match inputs so the counterpart probes see this window's packages;
# the reconciliation build after the probes is the authoritative one.
candidate_refresh_status=0
if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_CANDIDATE_REFRESH_TIMEOUT_SECONDS:-180}s" \
    uv run dbt run --no-partial-parse --project-dir dbt_project \
      --profiles-dir dbt_project \
      --select int_raw_response_versions int_raw_responses__current int_bill_candidates fct_bill_matches \
        int_hearings__congress int_hearing_committee_meeting_links fct_hearing_matches \
      || candidate_refresh_status=$?
  if (( candidate_refresh_status != 0 )); then
    status=1
  fi
fi

# Audit identifiers nested in newly harvested packages (the full member-reference
# expansion stays in the six-hour lane).
nested_identity_status=0
if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_IDENTITY_AUDIT_TIMEOUT_SECONDS:-360}s" \
    uv run dbt run --no-partial-parse --project-dir dbt_project \
      --profiles-dir dbt_project \
      --select int_govinfo_identity_fields fct_govinfo_nested_identity_exceptions \
      || nested_identity_status=$?
  if (( nested_identity_status != 0 )); then
    status=1
  fi
fi

nested_identity_test_status=0
if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_IDENTITY_TEST_TIMEOUT_SECONDS:-120}s" \
    uv run dbt test --no-partial-parse --project-dir dbt_project \
      --profiles-dir dbt_project \
      --select govinfo_nested_identity_audit_covers_payloads \
      || nested_identity_test_status=$?
  if (( nested_identity_test_status != 0 )); then
    status=1
  fi
fi

# Probe Congress.gov for GovInfo-only bills, so a publication lag becomes expiring
# 404 evidence instead of an open exception.
counterpart_status=0
if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_COUNTERPART_TIMEOUT_SECONDS:-300}s" \
    uv run python scripts/backfill_congress_bill_details.py \
      --limit "${BICAM_GOVINFO_COUNTERPART_LIMIT:-100}" \
      --concurrency "${BICAM_GOVINFO_COUNTERPART_CONCURRENCY:-2}" \
      --request-interval "${BICAM_GOVINFO_COUNTERPART_REQUEST_INTERVAL:-0.5}" \
      --key-wait-timeout "${BICAM_GOVINFO_COUNTERPART_KEY_WAIT_TIMEOUT:-600}" || counterpart_status=$?
  if (( counterpart_status != 0 )); then
    status=1
  fi
fi

# Same for CHRG/JHRG hearing packages.
hearing_counterpart_status=0
if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_HEARING_COUNTERPART_TIMEOUT_SECONDS:-300}s" \
    uv run python scripts/backfill_congress_govinfo_hearings.py \
    --congress-start "${BICAM_GOVINFO_HEARING_COUNTERPART_CONGRESS_START:-104}" \
    --congress-end "${BICAM_GOVINFO_HEARING_COUNTERPART_CONGRESS_END:-$current_congress}" \
      --limit "${BICAM_GOVINFO_HEARING_COUNTERPART_LIMIT:-100}" \
      --concurrency "${BICAM_GOVINFO_HEARING_COUNTERPART_CONCURRENCY:-2}" \
      --batch-size "${BICAM_GOVINFO_HEARING_COUNTERPART_BATCH_SIZE:-100}" \
      --request-interval "${BICAM_GOVINFO_HEARING_COUNTERPART_REQUEST_INTERVAL:-0.5}" \
      --key-wait-timeout "${BICAM_GOVINFO_HEARING_COUNTERPART_KEY_WAIT_TIMEOUT:-600}" \
      --active-key-budget "${BICAM_GOVINFO_HEARING_COUNTERPART_ACTIVE_KEY_BUDGET:-2}" || hearing_counterpart_status=$?
  if (( hearing_counterpart_status != 0 )); then
    status=1
  fi
fi

# Rebuild the bill, committee-report and hearing reconciliation graph.
reconciliation_status=0
if (( status == 0 )); then
  # The source-gap tail of this graph needs most of the service's hour.
  reconciliation_timeout_seconds=${BICAM_GOVINFO_RECONCILIATION_TIMEOUT_SECONDS:-1800}
  reconciliation_statement_timeout_ms=${BICAM_GOVINFO_RECONCILIATION_STATEMENT_TIMEOUT_MS:-1740000}
  PGAPPNAME="$dbt_app_name" \
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${reconciliation_statement_timeout_ms}" \
    timeout --signal=TERM --kill-after=20s "${reconciliation_timeout_seconds}s" \
    uv run dbt run --no-partial-parse --project-dir dbt_project \
      --profiles-dir dbt_project \
      --select int_bills__govinfo int_bill_candidates fct_bill_matches \
        +fct_committee_report_matches +fct_hearing_matches \
        +int_committee_report_candidates \
        +bridge_hearings__committee_meetings \
        fct_reconciliation_exceptions fct_reconciliation_metrics \
        bridge_govinfo__bill_versions \
        +fct_source_gap_queue bills_versions bills_version_links \
        bills bills_metadata committee_reports committee_reports_associated_bills \
        bills_reference_codes bills_reference_codes_sections \
        bills_reference_laws bills_reference_statutes \
        bills_reference_statutes_pages || { reconciliation_status=$?; bicam_stop_dbt_backends "$dbt_app_name"; }
  if (( reconciliation_status != 0 )); then
    status=1
  fi
fi

reconciliation_test_status=0
if (( reconciliation_status == 0 )); then
  reconciliation_test_timeout_seconds=${BICAM_GOVINFO_RECONCILIATION_TEST_TIMEOUT_SECONDS:-300}
  reconciliation_test_statement_timeout_ms=${BICAM_GOVINFO_RECONCILIATION_TEST_STATEMENT_TIMEOUT_MS:-270000}
  PGAPPNAME="$dbt_app_name" \
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${reconciliation_test_statement_timeout_ms}" \
    timeout --signal=TERM --kill-after=20s "${reconciliation_test_timeout_seconds}s" \
    uv run dbt test --no-partial-parse --project-dir dbt_project \
      --profiles-dir dbt_project \
      --select govinfo_bill_version_representatives_are_unique \
        govinfo_bill_version_metadata_propagates bills_versions_have_canonical_parents \
        bills_version_links_have_version_evidence bills_version_links_title_codes_match_source \
        bills_version_links_exclude_summary_codes \
        committee_reports_public_union_preserves_bill_side \
        committee_reports_associated_bills_cover_internal_union \
        source_gap_queue_open_rows_have_action \
      || { reconciliation_test_status=$?; bicam_stop_dbt_backends "$dbt_app_name"; }
  if (( reconciliation_test_status != 0 )); then
    status=1
  fi
fi

checkpoint_timestamp=$(date -u +%Y%m%dT%H%M%SZ)
checkpoint_path="$reports_root/govinfo-window-checkpoint-${checkpoint_timestamp}.md"
{
  echo "# GovInfo incremental-window checkpoint"
  echo
  echo "Generated: $(date -u '+%Y-%m-%d %H:%M UTC')"
  echo
  echo "Runner exit status: ${status}"
  echo "GovInfo bill identity refresh status: ${bill_identity_status}"
  echo "Current Congress resolution status: ${current_congress_status}"
  echo "Resolved current Congress: ${current_congress:-unresolved}"
echo "Congress counterpart probe status: ${counterpart_status}"
echo "Congress hearing counterpart probe status: ${hearing_counterpart_status}"
echo "Congress committee-report probe status: ${congress_report_probe_status}"
echo "Pre-probe candidate refresh status: ${candidate_refresh_status}"
echo "GovInfo nested identity audit status: ${nested_identity_status}"
echo "GovInfo nested identity test status: ${nested_identity_test_status}"
echo "Targeted reconciliation status: ${reconciliation_status}"
echo "Reconciliation targeted-test status: ${reconciliation_test_status}"
  echo
  echo '| Category | Completed through | Latest status | List pages | Fetch events | Failed events |'
  echo '|---|---|---|---:|---:|---:|'
  bicam_psql -At -F '|' -c \
    "with latest as (
       select distinct on (category)
              category, window_end, status, list_pages, fetch_events, failed_events
         from govinfo_collection_windows
        order by category, window_end desc, started_at desc
     )
     select category,
            to_char(window_end at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS UTC'),
            status,
            coalesce(list_pages,0), coalesce(fetch_events,0), coalesce(failed_events,0)
       from latest order by category;" 2>/dev/null | while IFS='|' read -r category frontier row_status pages events failures; do
        [ -n "$category" ] || continue
        echo "| ${category} | ${frontier} | ${row_status} | ${pages} | ${events} | ${failures} |"
      done
  echo
  echo "A shared frontier advances only when all four categories complete the exact same window."
} >"$checkpoint_path"

ln -sfn "$(basename "$checkpoint_path")" "$attempt_checkpoint"
if (( status == 0 )); then
  ln -sfn "$(basename "$checkpoint_path")" "$latest_checkpoint"
fi
echo "govinfo_window_checkpoint=${checkpoint_path}"
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) govinfo_incremental_windows_finished status=${status}"
exit "$status"
