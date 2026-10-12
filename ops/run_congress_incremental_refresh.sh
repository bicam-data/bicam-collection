#!/usr/bin/env bash
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

report_dir="$reports_root"
lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
cooldown_path="$report_dir/congress-api-cooldown-until"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
checkpoint="$report_dir/congress-incremental-checkpoint-$stamp.md"
latest="$report_dir/congress-incremental-checkpoint-latest.md"
attempt_latest="$report_dir/congress-incremental-checkpoint-attempt-latest.md"

mkdir -p "$report_dir"

if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -n -- "$@"
  # Another lane (often supplemental, on the same six-hour cadence) holds the
  # lock; the next timer run is the retry, so skip rather than queue behind it.
  printf '# Congress incremental checkpoint\n\n- generated: %s\n- status: lock_busy\n- note: another ingestion lane owns the shared lock; next scheduled run will retry\n' \
    "$(date -u --iso-8601=seconds)" >"$checkpoint"
  ln -sfn "$(basename "$checkpoint")" "$attempt_latest"
  exit 0
fi

now_epoch=$(date +%s)
cooldown_until=$(cat "$cooldown_path" 2>/dev/null || true)
if [[ "$cooldown_until" =~ ^[0-9]+$ ]] && (( cooldown_until > now_epoch )); then
  printf '# Congress incremental checkpoint\n\n- generated: %s\n- status: source_cooldown\n- retry_after_epoch: %s\n' \
    "$(date -u --iso-8601=seconds)" "$cooldown_until" >"$checkpoint"
  ln -sfn "$(basename "$checkpoint")" "$attempt_latest"
  exit 0
fi

cd "$project_root" || exit 1
bicam_load_project_env

# Stay under the observed aggregate rate boundary.
export BICAM_CONGRESS_GLOBAL_RATE_LIMIT="${BICAM_INCREMENTAL_GLOBAL_RATE_LIMIT:-18000}"
export BICAM_CONGRESS_PER_KEY_RATE_LIMIT="${BICAM_INCREMENTAL_PER_KEY_RATE_LIMIT:-20000}"
export DBT_PROFILES_DIR="$project_root/dbt_project"

started_at=$(date -u --iso-8601=seconds)
filtered_log=/tmp/bicam-congress-incremental-filtered.log
unfiltered_log=/tmp/bicam-congress-incremental-unfiltered.log
relationship_log=/tmp/bicam-congress-incremental-relationships.log
dbt_log=/tmp/bicam-congress-incremental-dbt.log

filtered_status=0
unfiltered_status=0
relationship_status=0
source_error_status=0
propagation_status=0
dbt_run_status=0
dbt_test_status=0
quality_status=0
congress_discovery_status=0
current_congress=""
discovery_log=/tmp/bicam-congress-incremental-discovery.log

# Refresh the Congress index first so a newly opened Congress is picked up as
# the current hearing partition (BICAM_CURRENT_CONGRESS overrides it).
.venv/bin/python scripts/run_congress_history.py \
  --categories congresses \
  --congress-start 1 --congress-end 1 \
  --mode refresh --max-pages 1000 --page-size 250 \
  --concurrency 2 --category-workers 1 \
  --key-wait-timeout 1200 \
  --request-interval "${BICAM_INCREMENTAL_REQUEST_INTERVAL:-0.20}" \
  >"$discovery_log" 2>&1 || congress_discovery_status=$?
if (( congress_discovery_status == 0 )); then
  current_congress=$(.venv/bin/python scripts/resolve_current_congress.py \
    ${BICAM_CURRENT_CONGRESS:+--override "$BICAM_CURRENT_CONGRESS"}) \
    || congress_discovery_status=$?
fi
if (( congress_discovery_status != 0 )); then
  current_congress=""
  echo "Unable to resolve the current Congress; the next scheduled run will retry." >>"$discovery_log"
fi

# These nine list endpoints accept fromDateTime/toDateTime. The overlap
# covers timestamp ties and late publication.
.venv/bin/python scripts/run_multi_crawl.py \
  --categories amendments bills committee-meetings committee-prints \
    committee-reports committees members nominations treaties \
  --mode incremental \
  --overlap-hours "${BICAM_INCREMENTAL_OVERLAP_HOURS:-24}" \
  --max-pages 1000 --page-size 250 \
  --concurrency "${BICAM_INCREMENTAL_CONCURRENCY:-8}" \
  --category-workers "${BICAM_INCREMENTAL_CATEGORY_WORKERS:-2}" \
  --key-wait-timeout 1200 \
  --request-interval "${BICAM_INCREMENTAL_REQUEST_INTERVAL:-0.20}" \
  >"$filtered_log" 2>&1 || filtered_status=$?

# Hearing lists have no date filter, so refresh just the current Congress.
if (( congress_discovery_status == 0 )); then
  .venv/bin/python scripts/run_congress_history.py \
    --categories hearings \
    --congress-start "$current_congress" --congress-end "$current_congress" \
    --mode refresh --max-pages 1000 --page-size 250 \
    --concurrency "${BICAM_INCREMENTAL_CONCURRENCY:-8}" --category-workers 1 \
    --key-wait-timeout 1200 \
    --request-interval "${BICAM_INCREMENTAL_REQUEST_INTERVAL:-0.20}" \
    --active-key-budget "${BICAM_INCREMENTAL_ACTIVE_KEY_BUDGET:-17}" \
    >"$unfiltered_log" 2>&1 || unfiltered_status=$?
else
  unfiltered_status=1
  printf 'current-hearing refresh skipped: current Congress resolution failed\n' >"$unfiltered_log"
fi

# Retry failures, repair list-chain gaps, follow hearing<->meeting links in
# both directions (the relationship is not symmetric), and re-probe the few
# detail URLs with deterministic source 500s.
{
  .venv/bin/python scripts/retry_failed_fetches.py \
    --categories amendments bills committee-meetings committee-prints \
      committee-reports committees hearings members nominations treaties \
    --limit 5000 --max-pages 1000 --page-size 250 --min-age-minutes 0 \
    --request-interval 0.5 --key-wait-timeout 1200 \
    || relationship_status=1
  .venv/bin/python scripts/run_root_gap_repair.py \
    --categories amendments bills committee-meetings committee-prints \
      committee-reports committees hearings nominations treaties \
    --page-size 250 --request-interval 0.5 --key-wait-timeout 1200 \
    --active-key-budget 4 --max-pages-per-partition 1000 \
    || relationship_status=1
  .venv/bin/python scripts/backfill_congress_meeting_hearings.py \
    --limit 5000 --concurrency 4 --batch-size 250 \
    --request-interval 0.5 --key-wait-timeout 1200 --active-key-budget 4 \
    || relationship_status=1
  .venv/bin/python scripts/backfill_congress_hearing_meetings.py \
    --limit 5000 --concurrency 4 --batch-size 250 \
    --request-interval 0.5 --key-wait-timeout 1200 --active-key-budget 4 \
    || relationship_status=1
  .venv/bin/python scripts/retry_failed_fetches.py \
    --categories amendments bills \
    --node-types amendment_detail bill_detail \
    --include-source-500 --only-source-500 \
    --limit "${BICAM_SOURCE_ERROR_RETRY_LIMIT:-25}" \
    --max-pages 1 --page-size 10 \
    --min-age-minutes "${BICAM_SOURCE_ERROR_RETRY_MIN_AGE_MINUTES:-120}" \
    --request-interval "${BICAM_SOURCE_ERROR_RETRY_REQUEST_INTERVAL:-0.5}" \
    --key-wait-timeout 1200 --no-descendants \
    || source_error_status=1
} >"$relationship_log" 2>&1

: >"$dbt_log"
# The model selection below does not pull in the raw intermediates, so build
# them first or new payloads stay invisible downstream.
timeout --signal=TERM --kill-after=30s 900s \
  uv run dbt run --no-partial-parse --project-dir dbt_project \
    --select int_raw_response_versions int_raw_responses__current \
  || propagation_status=$?

timeout --signal=TERM --kill-after=30s 300s \
  uv run dbt seed --no-partial-parse --project-dir dbt_project \
    --select govinfo_member_component_identity_overrides reconciliation_quality_thresholds bill_committee_report_multiplicity_allowlist --full-refresh \
    >>"$dbt_log" 2>&1 || dbt_run_status=$?

dbt_models=(
  int_congress_bill_text_versions
  int_congress_bill_text_formats
  int_congress_committee_report_records
  int_congress_committee_report_text_formats
  int_congress_bill_cosponsor_observations
  int_congress_nominee_observations
  nominations_nominees
  +fct_reconciliation_metrics
  +fct_source_gap_queue
  bills_versions
  bills_version_links
  bills_titles
  +bridge_hearings__committee_meetings
  +bridge_bills__committee_reports
  +bridge_members__legislation
  +bridge_committees__bills
  +fct_congress_internal_reciprocity_metrics
  +fct_govinfo_data_quality_metrics
  +fct_govinfo_committee_id_exceptions
  +fct_govinfo_nested_identity_exceptions
  +fct_govinfo_member_reference_identity_exceptions
  +fct_govinfo_member_identity_candidates
  +int_govinfo_multi_legislator_candidates
  fct_collection_coverage
  fct_root_detail_coverage
  bridge_govinfo__bill_versions
  +fct_committee_print_matches
  +fct_committee_print_reconciliation_metrics
  bridge_govinfo__committee_print_packages
  bridge_govinfo__member_components
  stg_govinfo__member_references
)
dbt_tests=(
  congress_internal_reciprocity_has_no_duplicate_edges
  congress_internal_reciprocity_status_is_consistent
  child_full_snapshot_precedes_incremental_empty_response
  incremental_child_windows_are_accumulated
  bill_report_part_asymmetries_are_classified
  nomination_child_part_ids_preserve_parts
  nominations_nominees_preserve_source_observations
  nominations_nominees_preserve_duplicate_ordinals
  nominations_nominees_have_no_repeated_public_rows
  bill_cosponsors_have_one_current_observation
  congress_member_amendment_reciprocity_has_both_directions
  congress_committee_report_reciprocity_has_both_directions
  committee_report_bill_union_preserves_directions
  committee_report_congress_union_includes_bill_side
  committee_report_candidates_one_per_canonical
  committee_reports_public_union_preserves_bill_side
  member_legislation_bridge_preserves_union
  committee_bill_bridge_preserves_union
  bills_titles_have_one_row_per_semantic_key
  govinfo_committee_id_overrides_target_observed
  fct_govinfo_member_identity_candidates
  govinfo_multi_legislator_components_are_preserved
  govinfo_multi_legislator_report_congress_precedence
  govinfo_nested_identity_audit_covers_payloads
  govinfo_committee_id_map_covers_nested_authority_ids
  govinfo_report_bill_member_matches_are_evidence_backed
  govinfo_member_reference_identity_exceptions_are_unresolved
  govinfo_leading_conjunction_member_refs_are_resolved
  committee_bill_subcommittee_asymmetries_are_classified
  govinfo_member_reference_source_ids_are_canonical
  govinfo_member_component_overrides_match_candidates
  govinfo_component_override_bioguide_ids_are_valid
  govinfo_member_candidate_chamber_is_consistent
  govinfo_nested_identity_context_is_consistent
  congress_internal_union_preserves_edge_directions
  reconciliation_unexcused_records_never_pass
  reconciliation_title_corroboration_meets_threshold
  govinfo_hearing_meeting_evidence_is_preserved
  hearing_committee_meeting_bridge_preserves_source_links
  hearing_committee_meeting_bridge_has_valid_ids
  committee_report_bill_bridge_preserves_union
  bill_committee_report_multiplicity_allowlist_is_current
  committee_print_bridge_preserves_every_package
  committee_print_canonical_ids_are_valid
  committee_print_collection_filter_is_explicit
  committee_print_reconciliation_counts_are_consistent
  govinfo_member_overrides_match_congress_terms
  bill_detail_duplicate_ids_are_historical
  congress_root_source_errors_have_identity_fallback
  raw_child_parent_lineage_is_unique
  raw_version_parent_lineage_propagates
  bill_text_parent_lineage_propagates
  committee_report_text_parent_lineage_propagates
  congress_bill_number_macro_normalizes_half
  congress_bill_numbers_have_no_unicode_fraction_suffix
  govinfo_bill_version_representatives_are_unique
  govinfo_bill_version_metadata_propagates
  bills_versions_have_canonical_parents
  bills_version_links_have_version_evidence
  bills_version_links_title_codes_match_source
  bills_version_links_exclude_summary_codes
  source_gap_queue_open_rows_have_action
)

# Run the targeted tests only; the full schema-test graph does not fit the window.
timeout --signal=TERM --kill-after=30s 1800s \
  uv run dbt run --no-partial-parse --project-dir dbt_project \
    --select "${dbt_models[@]}" \
    >>"$dbt_log" 2>&1 || dbt_run_status=$?

if (( dbt_run_status == 0 )); then
  timeout --signal=TERM --kill-after=30s 900s \
    uv run dbt test --no-partial-parse --project-dir dbt_project \
      --select "${dbt_tests[@]}" \
      >>"$dbt_log" 2>&1 || dbt_test_status=$?
fi
dbt_status=$(( dbt_run_status || dbt_test_status ))

timeout --signal=TERM --kill-after=30s 600s \
  .venv/bin/python scripts/check_quality_gates.py \
    --max-open-exceptions 0 --max-unsatisfied-fetches 0 \
    --max-missing-full-scope-partitions 0 \
    --max-missing-root-items 0 --max-missing-child-items 0 \
    >>"$dbt_log" 2>&1 || quality_status=$?

{
  echo "# Congress incremental checkpoint"
  echo
  echo "- started: $started_at"
  echo "- finished: $(date -u --iso-8601=seconds)"
  echo "- filtered incremental status: $filtered_status"
  echo "- Congress index/current-partition discovery status: $congress_discovery_status"
  echo "- resolved current Congress: ${current_congress:-unresolved}"
  echo "- current hearing/Congress refresh status: $unfiltered_status"
  echo "- hearing/meeting relationship status: $relationship_status"
  echo "- source-detail re-probe status: $source_error_status"
  echo "- raw intermediate propagation status: $propagation_status"
  echo "- dbt build status: $dbt_status"
  echo "- dbt model status: $dbt_run_status"
  echo "- dbt targeted-test status: $dbt_test_status"
  echo "- quality gate status: $quality_status"
  echo "- overlap hours: ${BICAM_INCREMENTAL_OVERLAP_HOURS:-24}"
  echo "- process request interval: ${BICAM_INCREMENTAL_REQUEST_INTERVAL:-0.20}s"
  log_tail "Filtered crawl tail" "$filtered_log" 30
  log_tail "Unfiltered crawl tail" "$unfiltered_log" 30
  log_tail "Relationship crawl tail" "$relationship_log" 30
  log_tail "Congress discovery tail" "$discovery_log" 30
  log_tail "dbt and quality tail" "$dbt_log" 50
} >"$checkpoint"
ln -sfn "$(basename "$checkpoint")" "$attempt_latest"

if (( congress_discovery_status || filtered_status || unfiltered_status || relationship_status || source_error_status || propagation_status || dbt_status || quality_status )); then
  exit 1
fi
ln -sfn "$(basename "$checkpoint")" "$latest"
