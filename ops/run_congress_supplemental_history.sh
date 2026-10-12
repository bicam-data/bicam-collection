#!/usr/bin/env bash
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

report_dir="$reports_root"
lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
checkpoint="$report_dir/congress-supplemental-history-$stamp.md"
latest="$report_dir/congress-supplemental-history-latest.md"
complete_marker="$report_dir/congress-supplemental-history-complete"
log_dir="$report_dir/congress-supplemental-history-work"
phase_status="$log_dir/phase-status.tsv"
targeted_log="$log_dir/targeted-$stamp.log"

mkdir -p "$report_dir" "$log_dir"
if [ -f "$complete_marker" ]; then
  exit 0
fi
lock_wait_seconds=${BICAM_SUPPLEMENTAL_LOCK_WAIT_SECONDS:-43200}
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
  printf '# Congress supplemental history\n\n- generated: %s\n- status: lock_timeout\n' \
    "$(date -u --iso-8601=seconds)" >"$checkpoint"
  ln -sfn "$(basename "$checkpoint")" "$latest"
  exit 1
fi

cd "$project_root" || exit 1
bicam_load_project_env
bicam_prefer_container_db_password
export DBT_PROFILES_DIR="$project_root/dbt_project"
export BICAM_CONGRESS_GLOBAL_RATE_LIMIT="${BICAM_SUPPLEMENTAL_GLOBAL_RATE_LIMIT:-18000}"
export BICAM_CONGRESS_PER_KEY_RATE_LIMIT="${BICAM_SUPPLEMENTAL_PER_KEY_RATE_LIMIT:-20000}"
request_interval="${BICAM_SUPPLEMENTAL_REQUEST_INTERVAL:-0.20}"

started_at=$(date -u --iso-8601=seconds)
printf '%s\t%s\t%s\n' "$started_at" run started >>"$phase_status"
targeted_status=0
pre_repair_coverage_status=0
root_gap_status=0
descendant_gap_status=0
hearing_meeting_status=0
retry_status=0
dbt_status=0
quality_status=0

.venv/bin/python scripts/run_congress_history.py \
  --categories bills amendments members congresses hearings committees \
    committee-meetings committee-prints committee-reports treaties nominations \
  --congress-start 1 --congress-end 119 \
  --mode backfill_descendants --max-pages 1000 --page-size 250 \
  --concurrency "${BICAM_SUPPLEMENTAL_CONCURRENCY:-12}" --category-workers 4 \
  --key-wait-timeout 1200 --request-interval "$request_interval" --active-key-budget 17 \
  >"$targeted_log" 2>&1 || targeted_status=$?
ln -sfn "$(basename "$targeted_log")" "$log_dir/targeted.log"
printf '%s\t%s\t%s\n' "$(date -u --iso-8601=seconds)" targeted "$targeted_status" >>"$phase_status"

# Refresh the positional ledger before targeted repair so deterministic
# Congress.gov source errors are preserved but not requeued as actionable
# gaps. This also ensures interior list holes use their exact offsets.
timeout --signal=TERM --kill-after=30s 900s \
  uv run dbt build --project-dir dbt_project \
    --select fct_pagination_coverage \
      pagination_identity_gaps_have_full_position_evidence \
      pagination_root_gaps_are_closed \
  >"$log_dir/pre-repair-coverage.log" 2>&1 || pre_repair_coverage_status=$?
printf '%s\t%s\t%s\n' "$(date -u --iso-8601=seconds)" pre_repair_coverage "$pre_repair_coverage_status" >>"$phase_status"

.venv/bin/python scripts/run_root_gap_repair.py \
  --categories bills amendments members congresses hearings committees \
    committee-meetings committee-prints committee-reports treaties nominations \
    --page-size 250 --max-pages-per-partition 1000 \
  --request-interval 0.5 --key-wait-timeout 1200 --active-key-budget 1 \
  >"$log_dir/root-gaps.log" 2>&1 || root_gap_status=$?
printf '%s\t%s\t%s\n' "$(date -u --iso-8601=seconds)" root_gaps "$root_gap_status" >>"$phase_status"

.venv/bin/python scripts/run_congress_descendant_refresh.py \
  --categories amendments bills committee-meetings committee-prints \
    committee-reports committees hearings members congresses nominations treaties \
  --batch-size 100 --concurrency 4 --page-size 250 --max-pages 1000 \
  --request-interval 0.5 --key-wait-timeout 1200 --active-key-budget 4 \
  --checkpoint-path "$report_dir/post-supplemental-descendant-checkpoint.json" \
  >"$log_dir/descendant-gaps.log" 2>&1 || descendant_gap_status=$?
printf '%s\t%s\t%s\n' "$(date -u --iso-8601=seconds)" descendant_gaps "$descendant_gap_status" >>"$phase_status"

.venv/bin/python scripts/backfill_congress_hearing_meetings.py \
  --limit 100 --concurrency 4 --batch-size 100 \
  --request-interval 0.5 --key-wait-timeout 1200 --active-key-budget 4 \
  >"$log_dir/hearing-meetings.log" 2>&1 || hearing_meeting_status=$?
printf '%s\t%s\t%s\n' "$(date -u --iso-8601=seconds)" hearing_meetings "$hearing_meeting_status" >>"$phase_status"

.venv/bin/python scripts/retry_failed_fetches.py \
  --categories amendments bills committee-meetings committee-prints committee-reports \
    committees hearings members nominations treaties \
  --limit 10000 --max-pages 1000 --page-size 250 --min-age-minutes 0 \
  --request-interval 0.5 --key-wait-timeout 1200 \
  >"$log_dir/retries.log" 2>&1 || retry_status=$?
printf '%s\t%s\t%s\n' "$(date -u --iso-8601=seconds)" retries "$retry_status" >>"$phase_status"

# The full coverage models are maintained by the bounded pre-repair phase.
# Keep the final reconciliation run focused on source models and quality
# outputs so a coverage-only scan cannot force the entire historical crawl to
# restart.  ``dbt build`` also schedules hundreds of legacy staging tests;
# several of those repeatedly group large raw-backed views and can hold the
# ingestion lock for tens of minutes.  Materialize the selected models first,
# then run the small, auditable reconciliation/identity test set below.
dbt_run_status=0
dbt_test_status=0
timeout --signal=TERM --kill-after=30s 300s \
  uv run dbt seed --no-partial-parse --project-dir dbt_project \
    --select govinfo_member_component_identity_overrides reconciliation_quality_thresholds --full-refresh \
    >"$log_dir/component-seed.log" 2>&1 || dbt_run_status=$?
timeout --signal=TERM --kill-after=30s 1800s \
  uv run dbt run --project-dir dbt_project \
    --select tag:bills tag:amendments tag:committee_meetings tag:committee_prints \
      tag:committee_reports tag:committees tag:hearings tag:members \
      tag:nominations tag:treaties \
      +fct_congress_internal_reciprocity_metrics \
      +bridge_members__legislation \
      +bridge_committees__bills \
      +fct_govinfo_data_quality_metrics \
      +fct_govinfo_member_identity_candidates \
      +fct_govinfo_nested_identity_exceptions \
      +fct_govinfo_committee_id_exceptions \
      +fct_govinfo_member_reference_identity_exceptions \
      +int_govinfo_multi_legislator_candidates \
      bridge_govinfo__member_components \
      govinfo_nested_identity_audit_covers_payloads \
  >"$log_dir/dbt.log" 2>&1 || dbt_run_status=$?
if (( dbt_run_status == 0 )); then
  timeout --signal=TERM --kill-after=30s 600s \
    uv run dbt test --project-dir dbt_project \
      --select congress_internal_reciprocity_has_no_duplicate_edges \
        congress_internal_reciprocity_status_is_consistent \
        congress_member_amendment_reciprocity_has_both_directions \
        govinfo_committee_id_overrides_target_observed \
        fct_govinfo_member_identity_candidates \
        govinfo_multi_legislator_components_are_preserved \
        govinfo_multi_legislator_report_congress_precedence \
        govinfo_nested_identity_audit_covers_payloads \
        govinfo_committee_id_map_covers_nested_authority_ids \
        govinfo_report_bill_member_matches_are_evidence_backed \
        govinfo_member_reference_identity_exceptions_are_unresolved \
        govinfo_member_reference_source_ids_are_canonical \
        govinfo_member_component_overrides_match_candidates \
        govinfo_component_override_bioguide_ids_are_valid \
        govinfo_member_candidate_chamber_is_consistent \
        govinfo_nested_identity_context_is_consistent \
        congress_internal_union_preserves_edge_directions \
        member_legislation_bridge_preserves_union \
        committee_bill_bridge_preserves_union \
        reconciliation_unexcused_records_never_pass \
        govinfo_member_overrides_match_congress_terms \
        bill_detail_duplicate_ids_are_historical \
        congress_root_source_errors_have_identity_fallback \
        raw_child_parent_lineage_is_unique \
        raw_version_parent_lineage_propagates \
        bill_text_parent_lineage_propagates \
        congress_bill_number_macro_normalizes_half \
        congress_bill_numbers_have_no_unicode_fraction_suffix \
    >>"$log_dir/dbt.log" 2>&1 || dbt_test_status=$?
fi
dbt_status=$(( dbt_run_status || dbt_test_status ))
printf '%s\t%s\t%s\n' "$(date -u --iso-8601=seconds)" dbt "$dbt_status" >>"$phase_status"

timeout --signal=TERM --kill-after=30s 900s \
  .venv/bin/python scripts/check_quality_gates.py \
    --max-open-exceptions 0 --max-unsatisfied-fetches 0 \
    --max-missing-full-scope-partitions 0 \
    --max-missing-root-items 0 --max-missing-child-items 0 \
  >>"$log_dir/dbt.log" 2>&1 || quality_status=$?
printf '%s\t%s\t%s\n' "$(date -u --iso-8601=seconds)" quality "$quality_status" >>"$phase_status"

{
  echo '# Congress supplemental historical checkpoint'
  echo
  echo "- started: $started_at"
  echo "- finished: $(date -u --iso-8601=seconds)"
  echo "- targeted Congress.gov families status: $targeted_status"
  echo "- pre-repair pagination coverage status: $pre_repair_coverage_status"
  echo "- root pagination repair status: $root_gap_status"
  echo "- committee/member/treaty descendant repair status: $descendant_gap_status"
  echo "- hearing-to-meeting repair status: $hearing_meeting_status"
  echo "- failed-fetch retry status: $retry_status"
  echo "- dbt model/test status: $dbt_status"
  echo "- quality gate status: $quality_status"
  for name in pre-repair-coverage root-gaps descendant-gaps hearing-meetings retries dbt; do
    echo
    echo "## $name tail"
    echo '```text'
    tail -n 35 "$log_dir/$name.log" 2>/dev/null || true
    echo '```'
  done
  echo
  echo "## targeted tail"
  echo '```text'
  tail -n 35 "$targeted_log" 2>/dev/null || true
  echo '```'
} >"$checkpoint"
ln -sfn "$(basename "$checkpoint")" "$latest"

if (( targeted_status || root_gap_status || descendant_gap_status || hearing_meeting_status || retry_status || dbt_status || quality_status )); then
  exit 1
fi
printf '%s\n' "$(date -u --iso-8601=seconds)" >"$complete_marker"
systemctl --user start --no-block bicam-congress-per-key-refresh-persistent.service || true
