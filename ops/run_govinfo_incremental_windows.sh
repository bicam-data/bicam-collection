#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

log_path="$reports_root/govinfo-incremental-windows.log"
lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
latest_checkpoint="$reports_root/govinfo-window-checkpoint-latest.md"
attempt_checkpoint="$reports_root/govinfo-window-checkpoint-attempt-latest.md"

mkdir -p "$reports_root"
exec >>"$log_path" 2>&1

# All ingestion lanes share one process-level lock.  The six-hour Congress
# refresh materializes reconciliation models after it crawls; allowing a
# GovInfo window to insert raw rows during that build can leave the quality
# gate observing a half-new cross-source state.  Wait for a bounded period when
# another lane owns the lock: a non-blocking skip can line up with the next
# hourly trigger and repeatedly delay the GovInfo frontier.  Waiting consumes
# no API quota and keeps the reconciliation build serialized.
# A six-hour Congress supplemental refresh can legitimately hold the shared
# lock for longer than the old 15-minute wait.  Queue the hourly GovInfo lane
# behind it instead of recording a lock-busy no-op and losing that window.
# This wait consumes no API quota; the service timeout remains the outer bound.
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
  # Keep the last successful frontier authoritative.  A coordination skip is
  # still durable, but it belongs under the attempt pointer so consumers do
  # not mistake a lock-busy no-op for a completed source window.
  ln -sfn "$(basename "$checkpoint_path")" "$attempt_checkpoint"
  echo "govinfo_incremental_windows_lock_busy=1"
  exit 0
fi

started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "$started_at govinfo_incremental_windows_started"
cd "$project_root" || exit 1

# The Python crawler loads the project dotenv through Settings, but dbt does
# not.  Load it explicitly and prefer the live container password so the
# post-window reconciliation step uses the same database credentials as the
# crawler and the other ingestion services.
bicam_load_project_env
export DBT_PROFILES_DIR="$project_root/dbt_project"

dbt_app_name="bicam-govinfo-dbt-$(date -u '+%Y%m%dT%H%M%SZ')-$$"
cancel_dbt_backends() {
  local app_name="$1"
  local cancel_attempt
  for cancel_attempt in 1 2; do
    bicam_psql -At -v app_name="$app_name" \
      <<< "select pg_cancel_backend(pid) from pg_stat_activity where application_name = :'app_name' and pid <> pg_backend_pid();" >/dev/null 2>&1 || true
    if (( cancel_attempt == 1 )); then
      sleep 2
    fi
  done
  # A backend in uninterruptible I/O can ignore cancellation long enough to
  # outlive the client. It is safe to terminate only this run's exact,
  # already-abandoned application name; never use a broad PID or dbt match.
  bicam_psql -At -v app_name="$app_name" \
    <<< "select pg_terminate_backend(pid) from pg_stat_activity where application_name = :'app_name' and pid <> pg_backend_pid();" >/dev/null 2>&1 || true
}

cleanup_dbt_backends() {
  local exit_status=$?
  cancel_dbt_backends "$dbt_app_name"
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

# Refresh the lightweight GovInfo bill identity model before probing.  The
# just-completed window may have inserted a package that was not visible to
# the previous materialized model.
bill_identity_status=0
if (( status == 0 )); then
  timeout --signal=TERM --kill-after=20s "${BICAM_GOVINFO_BILL_IDENTITY_TIMEOUT_SECONDS:-120}s" \
    uv run dbt run --no-partial-parse --project-dir dbt_project \
      --profiles-dir dbt_project --select int_bills__govinfo || bill_identity_status=$?
  if (( bill_identity_status != 0 )); then
    status=1
  fi
fi

# The Congress index is refreshed by the six-hour Congress lane. Resolve its
# highest observed detail partition here so the hourly CHRG/JHRG counterpart
# probe follows a newly opened Congress without a hard-coded upper bound.
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

# Probe the small, explicit set of GovInfo committee-report records whose
# Congress.gov counterpart is missing.  The exception model constructs the
# deterministic detail URL and retains 200/404 evidence; this bounded probe
# lets the managed source-gap queue move an open row to reviewed source-gap or
# back into reconciliation without waiting for a full Congress backfill.
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

# The window above inserts new raw GovInfo rows, while the counterpart helpers
# intentionally read the materialized candidate/match marts. Refresh those
# lightweight match inputs before probing so a newly published BILLS or CHRG
# package is discovered in this same window rather than one cycle later. The
# final reconciliation build below still runs after any counterpart payloads
# are fetched and therefore remains the authoritative post-probe materialization.
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

# Keep the nested GovInfo identity audit current for every newly harvested
# package.  This deliberately refreshes only the recursive identifier scan and
# its exception table; the much heavier all-category member-reference
# expansion remains in the six-hour Congress reconciliation lane.  Running
# this after ``int_raw_responses__current`` ensures Bioguide, Thomas, and
# authority values in package/granule subfields are checked in the same hourly
# window that fetched them.
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

# GovInfo can publish a BILLS package before the corresponding Congress.gov
# index page exposes that bill.  Probe those GovInfo-only identities in the
# same bounded run so a temporary publication lag becomes durable, expiring
# 404 evidence instead of an open reconciliation exception.  Existing 404s
# and 500s are skipped by the helper, so this does not replay the historical
# failure ledger or manufacture counterpart rows.
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

# GovInfo can also publish a CHRG/JHRG package before Congress.gov exposes the
# corresponding hearing detail.  Probe the bounded modern set immediately so
# a newly visible Congress hearing is linked in the same hourly cycle rather
# than waiting for the six-hour supplemental lane.  The helper skips existing
# links and recently confirmed 404s, so this is a frontier-driven probe, not a
# historical hearing sweep.
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

# Refresh the bounded cross-source reconciliation paths immediately after the
# counterpart probes.  The hourly window can publish committee-report or
# hearing packages as well as bills; selecting the parents of both match marts
# prevents newly visible GovInfo records from leaving an open exception until
# the six-hour release build.  The selector remains bounded to the affected
# reconciliation graph rather than running the full archive.
reconciliation_status=0
if (( status == 0 )); then
  # The resolver-backed source-gap graph can scan millions of immutable
  # response observations. Keep the server-side statement bound just inside
  # the client bound so a timeout cannot leave CREATE TABLE AS running after
  # this window exits.
  # The complete resolver-backed graph reaches the expensive source-gap tail
  # only after rebuilding the 3M-row child-state selection and observation
  # relations.  A 15-minute bound repeatedly stopped at model 73/77 and left
  # a mixed, non-certifying generation.  Keep the window bounded but allow the
  # graph to finish within the service's one-hour outer timeout.
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
        bills_reference_statutes_pages || { reconciliation_status=$?; cancel_dbt_backends "$dbt_app_name"; }
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
      || { reconciliation_test_status=$?; cancel_dbt_backends "$dbt_app_name"; }
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
