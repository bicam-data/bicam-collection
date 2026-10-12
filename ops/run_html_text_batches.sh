#!/usr/bin/env bash
set -euo pipefail

# Resumable HTML extraction lane. Each batch is small enough to stop safely;
# one lexical cursor per source table is persisted in
# reports/html-text/checkpoint.json and can be resumed after a restart or
# usage-window reset. This lane does not consume Congress.gov API keys because
# it reads public HTML renditions.
source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"
cd "$project_root"
lock_wait_seconds=${BICAM_HTML_LOCK_TIMEOUT_SECONDS:-900}
# `flock 9` by itself releases the lock when the helper process exits.  Run a
# guarded child instead so the lock remains held for extraction, dbt
# propagation, and reporting as one serialized checkpoint.
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  exec 9>/run/user/$(id -u)/bicam-ingestion.lock
  lock_status=0
  (
    flock -w "$lock_wait_seconds" -E 75 9 || exit $?
    export BICAM_INGESTION_LOCK_HELD=1
    exec "$0" "$@"
  ) || lock_status=$?
  if (( lock_status == 0 )); then
    exit 0
  else
    if (( lock_status != 75 )); then
      exit "$lock_status"
    fi
  fi
  busy_output_dir=${BICAM_HTML_REPORT_DIR:-"$reports_root/html-text"}
  mkdir -p "$busy_output_dir"
  printf '%s {"event":"lock_busy","retry":"next_timer"}\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >> "$busy_output_dir/run.log"
  echo "html_text_lock_busy; retrying on the next timer interval"
  exit 0
fi
bicam_load_project_env
output_dir=${BICAM_HTML_REPORT_DIR:-"$reports_root/html-text"}
batch_limit=${BICAM_HTML_BATCH_LIMIT:-500}
max_batches=${BICAM_HTML_MAX_BATCHES:-1}
concurrency=${BICAM_HTML_CONCURRENCY:-12}
request_interval=${BICAM_HTML_REQUEST_INTERVAL:-0.08}
congress_request_interval=${BICAM_HTML_CONGRESS_REQUEST_INTERVAL:-1.0}
govinfo_request_interval=${BICAM_HTML_GOVINFO_REQUEST_INTERVAL:-0.25}
refresh_days=${BICAM_HTML_REFRESH_DAYS:-30}
retry_failed_days=${BICAM_HTML_RETRY_FAILED_DAYS:-1}
request_timeout=${BICAM_HTML_REQUEST_TIMEOUT:-12}
max_bytes=${BICAM_HTML_MAX_BYTES:-20000000}
dbt_batch_interval=${BICAM_HTML_DBT_BATCH_INTERVAL:-6}
if ! [[ "$dbt_batch_interval" =~ ^[1-9][0-9]*$ ]]; then
  echo "invalid BICAM_HTML_DBT_BATCH_INTERVAL=$dbt_batch_interval (expected positive integer)" >&2
  exit 2
fi
mkdir -p "$output_dir"
result='{"requested": 0, "statuses": {}}'

checkpoint="$output_dir/checkpoint.json"

# Rotate one public text table per batch. The former single lexical cursor
# could spend days in the hearing URL range before reaching bills, amendments,
# or GovInfo bill versions. Successful rows are filtered by the database, so an old
# checkpoint is safely restarted at the beginning of the fair queue.
source_tables=(amendments_texts bills_texts committee_prints_texts committee_reports_texts hearings_texts bills_versions)
next_source_table="${source_tables[0]}"
declare -A last_urls
pending_dbt=0
dbt_batches_since_last=0
for table in "${source_tables[@]}"; do
  last_urls["$table"]=""
done
if [[ -s "$checkpoint" ]]; then
  while IFS=$'\t' read -r key value; do
    case "$key" in
      next_source_table) [[ -n "$value" ]] && next_source_table="$value" ;;
      pending_dbt) [[ "$value" == "1" ]] && pending_dbt=1 ;;
      dbt_batches_since_last) [[ "$value" =~ ^[0-9]+$ ]] && dbt_batches_since_last="$value" ;;
      last_url:*)
        table="${key#last_url:}"
        [[ -v last_urls["$table"] ]] && last_urls["$table"]="$value"
        ;;
    esac
  done < <(uv run python - "$checkpoint" <<'PY'
import json, sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text())
tables = payload.get("last_urls")
if isinstance(tables, dict):
    for table, value in tables.items():
        print(f"last_url:{table}\t{value or ''}")
next_table = payload.get("next_source_table")
if next_table:
    print(f"next_source_table\t{next_table}")
if payload.get("pending_dbt"):
    print("pending_dbt\t1")
if isinstance(payload.get("dbt_batches_since_last"), int) and payload["dbt_batches_since_last"] >= 0:
    print(f"dbt_batches_since_last\t{payload['dbt_batches_since_last']}")
PY
  )
fi
table_index=0
if [[ ! -v last_urls["$next_source_table"] ]]; then
  next_source_table="${source_tables[0]}"
fi
for index in "${!source_tables[@]}"; do
  if [[ "${source_tables[$index]}" == "$next_source_table" ]]; then
    table_index="$index"
    break
  fi
done
after="${last_urls[$next_source_table]}"

# Count only full/useful batches against the configured cap.  A short batch
# means that table is exhausted (or has no currently eligible URLs), so it is
# reset and skipped without making the next useful table wait for another
# timer tick.  The finite attempt bound prevents an all-exhausted queue from
# turning the fairness loop into an infinite cycle.
useful_batches=0
max_attempts=$((max_batches + ${#source_tables[@]}))
for ((attempt=1; attempt<=max_attempts && useful_batches<max_batches; attempt++)); do
  current_source_table="$next_source_table"
  args=(scripts/fetch_html_text.py --source-table "$current_source_table" --limit "$batch_limit" --concurrency "$concurrency" --request-interval "$request_interval" --congress-request-interval "$congress_request_interval" --govinfo-request-interval "$govinfo_request_interval" --refresh-days "$refresh_days" --retry-failed-days "$retry_failed_days" --timeout "$request_timeout" --max-bytes "$max_bytes")
  [[ -n "$after" ]] && args+=(--after "$after")
  result=$(uv run python "${args[@]}")
  printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$result" >> "$output_dir/run.log"
  last_url=$(uv run python - "$result" <<'PY'
import json, sys
print(json.loads(sys.argv[1]).get("last_url") or "")
PY
  )
  batch_successful=$(uv run python - "$result" <<'PY'
import json, sys
print(json.loads(sys.argv[1]).get("statuses", {}).get("success", 0))
PY
  )
  if (( batch_successful > 0 )); then
    pending_dbt=1
    dbt_batches_since_last=$((dbt_batches_since_last + 1))
  fi
  last_urls["$current_source_table"]="$last_url"
  table_index=$(( (table_index + 1) % ${#source_tables[@]} ))
  next_source_table="${source_tables[$table_index]}"
  uv run python - "$checkpoint" "$result" "$next_source_table" "$pending_dbt" "$dbt_batches_since_last" "${last_urls[amendments_texts]}" "${last_urls[bills_texts]}" "${last_urls[committee_prints_texts]}" "${last_urls[committee_reports_texts]}" "${last_urls[hearings_texts]}" "${last_urls[bills_versions]}" <<'PY'
import json, sys
from pathlib import Path
payload = json.loads(sys.argv[2])
payload["next_source_table"] = sys.argv[3]
payload["pending_dbt"] = int(sys.argv[4])
payload["dbt_batches_since_last"] = int(sys.argv[5])
payload["last_urls"] = {
    "amendments_texts": sys.argv[6],
    "bills_texts": sys.argv[7],
    "committee_prints_texts": sys.argv[8],
    "committee_reports_texts": sys.argv[9],
    "hearings_texts": sys.argv[10],
    "bills_versions": sys.argv[11],
}
payload.pop("last_url", None)
Path(sys.argv[1]).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  after="${last_urls[$next_source_table]}"
  requested=$(uv run python - "$result" <<'PY'
import json, sys
print(json.loads(sys.argv[1]).get("requested", 0))
PY
  )
  if (( requested < batch_limit )); then
    # A complete pass for this table starts from the beginning next time so
    # newly added URLs and URLs past the refresh age are reconsidered.  Keep
    # the bounded invocation moving, though: an exhausted table must not
    # consume the remaining rounds and delay the next table until the next
    # timer tick.
    last_urls["$current_source_table"]=""
    uv run python - "$checkpoint" "${last_urls[amendments_texts]}" "${last_urls[bills_texts]}" "${last_urls[committee_prints_texts]}" "${last_urls[committee_reports_texts]}" "${last_urls[hearings_texts]}" "${last_urls[bills_versions]}" <<'PY'
import json, sys
from pathlib import Path
path = Path(sys.argv[1])
payload = json.loads(path.read_text())
payload.setdefault("last_urls", {})
for table, value in zip(("amendments_texts", "bills_texts", "committee_prints_texts", "committee_reports_texts", "hearings_texts", "bills_versions"), sys.argv[2:], strict=True):
    payload["last_urls"][table] = value
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
    continue
  fi
  useful_batches=$((useful_batches + 1))
done

# Propagate newly extracted text into the public tables in the same serialized
# checkpoint. A batch containing only HTTP/fetch failures cannot change the
# public tables, so skip the expensive rebuild in that case.
dbt_cycle_boundary=0
[[ "$next_source_table" == "${source_tables[0]}" ]] && dbt_cycle_boundary=1
if [[ "${BICAM_HTML_SKIP_DBT:-0}" != "1" ]] && (( pending_dbt == 1 )) \
    && (( dbt_batches_since_last >= dbt_batch_interval \
          || "${BICAM_HTML_DBT_FORCE:-0}" == "1" \
          || (dbt_cycle_boundary == 1 && dbt_batches_since_last > 0) )); then
  # Rebuild each text model's selected child-state ancestors as well.  The
  # public text tables depend on resolver-backed intermediate tables (most
  # visibly committee-print text); selecting only the leaf can otherwise leave
  # a freshly harvested raw response invisible to the public product.
  # Keep this graph explicit instead of selecting every ancestor of each leaf:
  # the ``+bills_texts`` form also rebuilds the entire bills/reconciliation
  # graph, which makes a bounded HTML checkpoint spend its timeout on unrelated
  # title and match models. These are the raw/current, shared child-state,
  # text-specific, and public relations required for the six HTML sources;
  # root/public identity tables are refreshed by their source lanes.
  html_dbt_select=(
    int_raw_response_versions int_raw_responses__current
    int_congress_child_response_selection int_congress_child_response_selections
    int_congress_amendment_text_versions int_congress_amendment_text_formats
    stg_congress__amendment_texts amendments_texts
    int_congress_bill_text_versions int_congress_bill_text_formats
    stg_congress__bill_texts bills_texts
    int_congress_committee_print_text_formats stg_congress__committee_print_texts
    int_committee_prints__congress int_committee_print_public_id_map committee_prints_texts
    int_congress_committee_report_records int_congress_committee_report_text_formats
    stg_congress__committee_report_detail stg_congress__bill_committeereports
    stg_congress__committee_report_texts int_committee_reports__congress
    int_committee_reports__govinfo committee_reports_texts
    int_hearings__congress int_congress_hearing_public_id_map
    int_govinfo_hearing_identity_map int_govinfo_hearing_granules
    stg_congress__hearing_texts hearings_texts
    bills_versions
  )
  # Match the client timeout with a slightly earlier PostgreSQL statement
  # timeout so cancellation cannot leave a server-side table build running
  # after this bounded checkpoint exits.
  # The resolver-backed bill-text-version model needs more than ten minutes
  # on the production archive. Keep propagation bounded, but allow one
  # complete graph attempt to finish rather than repeatedly cancelling during
  # its largest table. PostgreSQL still stops the statement before the client
  # timeout, and the cursor remains durable if either bound is reached.
  html_dbt_timeout_seconds=${BICAM_HTML_DBT_TIMEOUT_SECONDS:-1800}
  html_dbt_statement_timeout_ms=${BICAM_HTML_DBT_STATEMENT_TIMEOUT_MS:-1740000}
  html_dbt_app_name="bicam-html-dbt-$(date -u '+%Y%m%dT%H%M%SZ')-$$"
  html_dbt_test_app_name="${html_dbt_app_name}-test"
  cancel_html_dbt_backends() {
    local app_name="$1"
    # A client-side timeout can leave a PostgreSQL backend alive until TCP
    # keepalive notices the disconnect. Cancel only this checkpoint's unique
    # application name; never touch another ingestion lane.
    for cancel_attempt in 1 2; do
      bicam_psql -At -v app_name="$app_name" \
        <<< "select pg_cancel_backend(pid) from pg_stat_activity where application_name = :'app_name' and pid <> pg_backend_pid();" >/dev/null 2>&1 || true
      # The first cancellation can race the client-side timeout while its
      # connection is still unwinding.  A short second pass catches that
      # backend without broad termination or touching another run.
      (( cancel_attempt == 2 )) || sleep 2
    done
    # A backend in uninterruptible I/O can survive cancellation after the
    # client has exited. Terminate only this exact abandoned application name.
    bicam_psql -At -v app_name="$app_name" \
      <<< "select pg_terminate_backend(pid) from pg_stat_activity where application_name = :'app_name' and pid <> pg_backend_pid();" >/dev/null 2>&1 || true
  }
  cleanup_html_dbt_backends() {
    local exit_status=$?
    if [[ -n "${html_dbt_app_name:-}" ]]; then
      cancel_html_dbt_backends "$html_dbt_app_name"
    fi
    return "$exit_status"
  }
  # A systemd start-timeout can terminate the wrapper after its child client
  # exits.  Keep the exact application name available to an EXIT/TERM trap so
  # that path cannot leave an unowned server-side DBT backend behind.
  trap cleanup_html_dbt_backends EXIT
  trap 'exit 143' TERM
  trap 'exit 130' INT
  html_dbt_status=0
  if PGAPPNAME="$html_dbt_app_name" \
      PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${html_dbt_statement_timeout_ms}" \
      timeout --signal=TERM --kill-after=20s "${html_dbt_timeout_seconds}s" \
      uv run dbt run --no-partial-parse --project-dir dbt_project --profiles-dir dbt_project \
        --select "${html_dbt_select[@]}"; then
    :
  else
    html_dbt_status=$?
    cancel_html_dbt_backends "$html_dbt_app_name"
  fi
  html_dbt_test_timeout_seconds=${BICAM_HTML_DBT_TEST_TIMEOUT_SECONDS:-300}
  html_dbt_test_statement_timeout_ms=${BICAM_HTML_DBT_TEST_STATEMENT_TIMEOUT_MS:-270000}
  html_dbt_test_status=0
  if (( html_dbt_status == 0 )); then
    if PGAPPNAME="$html_dbt_test_app_name" \
        PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${html_dbt_test_statement_timeout_ms}" \
        timeout --signal=TERM --kill-after=20s "${html_dbt_test_timeout_seconds}s" \
        uv run dbt test --no-partial-parse --project-dir dbt_project --profiles-dir dbt_project \
          --select html_successful_extractions_have_text html_successful_current_prefers_last_success; then
      :
    else
      html_dbt_test_status=$?
      cancel_html_dbt_backends "$html_dbt_test_app_name"
    fi
  else
    echo "html_dbt_test_skipped=1 reason=dbt_run_status_${html_dbt_status}"
  fi
  echo "html_dbt_run_status=$html_dbt_status"
  echo "html_dbt_test_status=$html_dbt_test_status"
  if (( html_dbt_status == 0 && html_dbt_test_status == 0 )); then
    uv run python - "$checkpoint" <<'PY'
import json, sys
from pathlib import Path
path = Path(sys.argv[1])
payload = json.loads(path.read_text())
payload["pending_dbt"] = 0
payload["dbt_batches_since_last"] = 0
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  else
    # Keep the extraction cursor and the pending flag durable.  The next
    # checkpoint retries propagation without discarding successfully fetched
    # HTML or pretending the public models are current.
    echo "html_dbt_pending=1 reason=propagation_failed"
    # Avoid retrying the same expensive graph on every short extraction tick.
    # New successful batches advance the counter; BICAM_HTML_DBT_FORCE=1 can
    # still request an immediate refresh.
    dbt_batches_since_last=0
    uv run python - "$checkpoint" <<'PY'
import json, sys
from pathlib import Path
path = Path(sys.argv[1])
payload = json.loads(path.read_text())
payload["dbt_batches_since_last"] = 0
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  fi
elif (( pending_dbt == 1 )); then
  echo "html_dbt_deferred=1 batches_since_last=$dbt_batches_since_last interval=$dbt_batch_interval"
fi

# Keep a per-public-table checkpoint alongside the URL cursor.  This is
# intentionally non-fatal: extraction results and the cursor are already
# durable, and a report query must not stop the next batch.
uv run python scripts/report_html_text_progress.py --output-dir "$output_dir" || true

echo "html_text_checkpoint=$checkpoint"
