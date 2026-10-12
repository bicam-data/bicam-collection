#!/usr/bin/env bash
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

report_dir="$reports_root"
lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
checkpoint="$report_dir/congress-supplemental-refresh-$stamp.md"
latest="$report_dir/congress-supplemental-refresh-latest.md"
attempt_latest="$report_dir/congress-supplemental-refresh-attempt-latest.md"
log=/tmp/bicam-congress-supplemental-refresh-$stamp.log

mkdir -p "$report_dir"
lock_wait_seconds=${BICAM_SUPPLEMENTAL_LOCK_WAIT_SECONDS:-900}
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
  # The incremental lane and this broader lane are intentionally serialized.
  # A bounded wait lets a timer collision complete after the active lane rather
  # than silently deferring the targeted refresh for the entire six-hour cycle.
  printf '# Congress supplemental refresh\n\n- generated: %s\n- status: lock_busy\n- note: another ingestion lane owns the shared lock; next scheduled run will retry\n' \
    "$(date -u --iso-8601=seconds)" >"$checkpoint"
  ln -sfn "$(basename "$checkpoint")" "$attempt_latest"
  exit 0
fi

cd "$project_root" || exit 1
bicam_load_project_env
export DBT_PROFILES_DIR="$project_root/dbt_project"
export BICAM_CONGRESS_GLOBAL_RATE_LIMIT="${BICAM_SUPPLEMENTAL_GLOBAL_RATE_LIMIT:-18000}"
export BICAM_CONGRESS_PER_KEY_RATE_LIMIT="${BICAM_SUPPLEMENTAL_PER_KEY_RATE_LIMIT:-20000}"

# Give every dbt connection in this wrapper a run-unique application name so
# a client timeout can cancel only its own orphaned PostgreSQL backends.
dbt_app_name="bicam-supplemental-dbt-${stamp}"
cancel_dbt_backends() {
  local app_name="$1"
  bicam_psql -At -v app_name="$app_name" \
    <<< "select pg_cancel_backend(pid) from pg_stat_activity where application_name = :'app_name' and pid <> pg_backend_pid();" >/dev/null 2>&1 || true
}

# Keep the scheduled lane conservative by default, while allowing an
# operator to raise throughput when the per-key headers and system health
# support it.  These knobs are deliberately scoped to this service; they do
# not alter other crawlers or the source-wide availability guard.
refresh_concurrency="${BICAM_SUPPLEMENTAL_CONCURRENCY:-8}"
refresh_category_workers="${BICAM_SUPPLEMENTAL_CATEGORY_WORKERS:-2}"
refresh_request_interval="${BICAM_SUPPLEMENTAL_REQUEST_INTERVAL:-0.20}"
refresh_descendant_concurrency="${BICAM_SUPPLEMENTAL_DESCENDANT_CONCURRENCY:-4}"
refresh_descendant_interval="${BICAM_SUPPLEMENTAL_DESCENDANT_INTERVAL:-0.5}"
refresh_descendant_max_batches="${BICAM_SUPPLEMENTAL_DESCENDANT_MAX_BATCHES:-100}"
feature_parity_concurrency="${BICAM_SUPPLEMENTAL_FEATURE_PARITY_CONCURRENCY:-8}"
feature_parity_interval="${BICAM_SUPPLEMENTAL_FEATURE_PARITY_INTERVAL:-0.2}"
feature_parity_active_keys="${BICAM_SUPPLEMENTAL_FEATURE_PARITY_ACTIVE_KEYS:-12}"

started_at=$(date -u --iso-8601=seconds)
status=0
{
  # Hearings and Congresses do not support update-date filters.  Keep them out
  # of this all-history incremental discovery pass; the bounded Congress-119
  # refresh below is the intentional unfiltered operation for those families.
  .venv/bin/python scripts/run_multi_crawl.py \
    --categories amendments bills committee-meetings committee-prints \
      committee-reports committees members nominations treaties \
    --mode incremental --overlap-hours 24 \
    --max-pages 1000 --page-size 250 --concurrency "$refresh_concurrency" \
    --category-workers "$refresh_category_workers" \
    --key-wait-timeout 1200 --request-interval "$refresh_request_interval" || status=1

  # Refresh the unpartitioned Congress index first, then resolve the highest
  # observed Congress so the unfiltered hearing refresh follows a newly opened
  # Congress automatically. BICAM_CURRENT_CONGRESS is an explicit recovery
  # override; a failed resolution stops only this lane and is retried later.
  congress_discovery_status=0
  current_congress=""
  .venv/bin/python scripts/run_congress_history.py \
    --categories congresses \
    --congress-start 1 --congress-end 1 --mode refresh \
    --max-pages 1000 --page-size 250 --concurrency 2 --category-workers 1 \
    --key-wait-timeout 1200 --request-interval "$refresh_request_interval" || congress_discovery_status=$?
  if (( congress_discovery_status == 0 )); then
    if [[ -n "${BICAM_CURRENT_CONGRESS:-}" ]]; then
      current_congress="$({ .venv/bin/python scripts/resolve_current_congress.py --override "$BICAM_CURRENT_CONGRESS"; })" \
        || congress_discovery_status=$?
    else
      current_congress="$({ .venv/bin/python scripts/resolve_current_congress.py; })" \
        || congress_discovery_status=$?
    fi
  fi
  if (( congress_discovery_status == 0 )); then
    .venv/bin/python scripts/run_congress_history.py \
      --categories hearings \
      --congress-start "$current_congress" --congress-end "$current_congress" --mode full \
      --max-pages 1000 --page-size 250 --concurrency "$refresh_concurrency" --category-workers 1 \
      --key-wait-timeout 1200 --request-interval "$refresh_request_interval" --active-key-budget 12 || status=1
  else
    echo "current-hearing refresh skipped: current Congress resolution failed"
    status=1
  fi

  # Feature-parity backfill: nominee-group endpoints are independently
  # paginated, and historical amendment text children remain incomplete.
  # Give those known source paths a stable cursor of their own so they make
  # progress every cycle instead of waiting behind lower-priority categories.
  .venv/bin/python scripts/run_congress_descendant_refresh.py \
    --categories nominations amendments \
    --batch-size 100 --concurrency "$feature_parity_concurrency" \
    --page-size 250 --max-pages 1000 \
    --max-batches "${BICAM_SUPPLEMENTAL_FEATURE_PARITY_MAX_BATCHES:-100}" \
    --request-interval "$feature_parity_interval" --key-wait-timeout 1200 \
    --active-key-budget "$feature_parity_active_keys" \
    --checkpoint-path "$report_dir/feature-parity-descendant-checkpoint.json" || status=1

  # A completed descendant cursor can miss a child URL that was advertised by
  # a newly refreshed bill detail after the cursor passed that root.  Replay
  # only the finite set proven missing by the same edge grammar used by the
  # coverage audit; the broad stored-root sweep below remains the fallback for
  # any residual gap.  This keeps the common repair bounded to a few hundred
  # requests instead of replaying hundreds of thousands of bill roots.
  targeted_child_status=0
  timeout --signal=TERM --kill-after=30s 1200s \
    .venv/bin/python scripts/backfill_missing_child_endpoints.py \
      --max-items "${BICAM_SUPPLEMENTAL_MISSING_CHILD_MAX_ITEMS:-300}" \
      --concurrency "${BICAM_SUPPLEMENTAL_MISSING_CHILD_CONCURRENCY:-8}" \
      --page-size 250 --max-pages 1000 \
      --request-interval "${BICAM_SUPPLEMENTAL_MISSING_CHILD_INTERVAL:-0.25}" \
      --key-wait-timeout 1200 --active-key-budget "${BICAM_SUPPLEMENTAL_MISSING_CHILD_ACTIVE_KEYS:-12}" \
      || targeted_child_status=$?
  echo "targeted_missing_child_backfill_status=$targeted_child_status"
  if (( targeted_child_status != 0 )); then
    status=1
  fi

  # Descendant refresh is driven by the child-coverage ledger.  The audit is a
  # read-only database check and is substantially cheaper than traversing
  # stored roots; do not spend API quota on an unconditional sweep when it
  # already proves that every declared child edge is present or classified.
  child_audit_status=0
  child_audit_log=/tmp/bicam-child-coverage-supplemental-$stamp.log
  timeout --signal=TERM --kill-after=20s 600s \
    .venv/bin/python scripts/check_child_coverage.py \
      --output "$report_dir/child-coverage-checkpoint-latest.md" \
      --history-dir "$report_dir/child-coverage-history" \
      >"$child_audit_log" 2>&1 || child_audit_status=$?
  cat "$child_audit_log"
  if (( child_audit_status != 0 )); then
    echo "child_coverage_audit_failed=$child_audit_status"
    status=1
  else
    child_missing=$(awk -F= '$1=="child_coverage_missing_total" {print $2}' "$child_audit_log" | tail -n 1)
    child_missing=${child_missing:-unknown}
    echo "child_coverage_missing_total=$child_missing"
    if [[ ! "$child_missing" =~ ^[0-9]+$ ]]; then
      echo "child_coverage_audit_unparseable=1"
      status=1
    elif [ "$child_missing" = "0" ]; then
      echo "backfill_descendant_refresh_skipped=1 reason=child_coverage_complete"
    else
      .venv/bin/python scripts/run_congress_descendant_refresh.py \
        --categories amendments bills committee-meetings committee-prints \
          committee-reports committees hearings members congresses nominations treaties \
        --batch-size 100 --concurrency "$refresh_descendant_concurrency" --page-size 250 --max-pages 1000 \
        --max-batches "$refresh_descendant_max_batches" \
        --request-interval "$refresh_descendant_interval" --key-wait-timeout 1200 --active-key-budget 4 \
        --checkpoint-path "$report_dir/post-supplemental-descendant-checkpoint.json" || status=1
    fi
  fi

  # Propagate the crawl's current raw state into the reconciliation inputs
  # before probing the opposite source.  The final product rebuild below is
  # still authoritative, but waiting until then makes newly discovered
  # Congress-only records invisible to the targeted GovInfo probes in this
  # cycle.  Keep this deliberately narrow so an incremental run does not pay
  # for a second full product rebuild merely to construct probe candidates.
  pre_reconcile_status=0
  # Keep PostgreSQL's server-side timeout just inside the client timeout.  A
  # client-side timeout alone can leave a CREATE TABLE AS query running after
  # the dbt process exits, consuming the shared database for the next lane.
  PGAPPNAME="$dbt_app_name" \
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${BICAM_SUPPLEMENTAL_PRE_RECONCILE_STATEMENT_TIMEOUT_MS:-840000}" \
    timeout --signal=TERM --kill-after=30s 900s \
      uv run dbt run --no-partial-parse --project-dir dbt_project \
      --select int_raw_response_versions int_raw_responses__current \
        int_congress_bill_text_versions int_congress_bill_text_formats \
        int_congress_committee_report_records int_congress_committee_report_text_formats \
        int_congress_bill_cosponsor_observations \
        int_congress_nominee_observations nominations_nominees \
        bills_titles \
        int_bills__congress int_bills__govinfo int_bill_candidates fct_bill_matches \
        int_committee_reports__congress int_committee_reports__govinfo \
        int_committee_report_candidates fct_committee_report_matches \
      || { pre_reconcile_status=$?; cancel_dbt_backends "$dbt_app_name"; }
  echo "pre_reconciliation_propagation_status=$pre_reconcile_status"
  if (( pre_reconcile_status != 0 )); then
    status=1
  fi

  # Reconcile the opposite source directions as well.  These helpers derive
  # deterministic counterpart URLs from current source records, retain 404
  # evidence, and skip recently probed endpoints; they are bounded probes, not
  # historical crawls.
  .venv/bin/python scripts/backfill_govinfo_bills.py \
    --modern-missing \
    --limit "${BICAM_SUPPLEMENTAL_GOVINFO_BILL_PROBE_LIMIT:-500}" \
    --concurrency "${BICAM_SUPPLEMENTAL_GOVINFO_BILL_PROBE_CONCURRENCY:-3}" \
    --request-interval "${BICAM_SUPPLEMENTAL_GOVINFO_BILL_PROBE_INTERVAL:-0.25}" \
    --key-wait-timeout 1200 \
    --retry-404-after-hours "${BICAM_SUPPLEMENTAL_GOVINFO_BILL_PROBE_RETRY_404_HOURS:-168}" || status=1
  .venv/bin/python scripts/backfill_govinfo_committee_reports.py \
    --limit "${BICAM_SUPPLEMENTAL_GOVINFO_REPORT_PROBE_LIMIT:-500}" \
    --concurrency "${BICAM_SUPPLEMENTAL_GOVINFO_REPORT_PROBE_CONCURRENCY:-3}" \
    --retry-404-after-hours "${BICAM_SUPPLEMENTAL_GOVINFO_REPORT_PROBE_RETRY_404_HOURS:-168}" || status=1
  if (( congress_discovery_status == 0 )); then
    .venv/bin/python scripts/backfill_congress_govinfo_hearings.py \
      --congress-start 104 --congress-end "$current_congress" \
      --limit "${BICAM_SUPPLEMENTAL_GOVINFO_HEARING_PROBE_LIMIT:-500}" \
      --concurrency "${BICAM_SUPPLEMENTAL_GOVINFO_HEARING_PROBE_CONCURRENCY:-2}" \
      --batch-size 100 --request-interval "${BICAM_SUPPLEMENTAL_GOVINFO_HEARING_PROBE_INTERVAL:-0.5}" \
      --key-wait-timeout 1200 || status=1
  else
    echo "GovInfo hearing counterpart probe skipped: current Congress resolution failed"
    status=1
  fi

  .venv/bin/python scripts/retry_failed_fetches.py \
    --categories amendments bills committee-meetings committee-prints committee-reports \
      committees hearings members nominations treaties \
    --limit 2000 --max-pages 1000 --page-size 250 --min-age-minutes 0 \
    --request-interval 0.5 --key-wait-timeout 1200 || status=1

  # Recheck only the small, classified root-detail source-error set on each
  # six-hour pass.  Ordinary retries intentionally exclude known 500s so a
  # malformed upstream record cannot consume the whole API budget; this
  # bounded opt-in probe still heals a record automatically if Congress.gov
  # repairs it, without replaying historical list pages or child fan-out.
  .venv/bin/python scripts/retry_failed_fetches.py \
    --categories amendments bills \
    --node-types amendment_detail bill_detail \
    --only-source-500 --limit 25 --min-age-minutes 360 \
    --no-descendants \
    --request-interval 1.0 --key-wait-timeout 1200 || status=1

  PGAPPNAME="$dbt_app_name" \
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${BICAM_SUPPLEMENTAL_SEED_STATEMENT_TIMEOUT_MS:-270000}" \
    timeout --signal=TERM --kill-after=30s 300s \
    uv run dbt seed --no-partial-parse --project-dir dbt_project \
      --select govinfo_member_component_identity_overrides reconciliation_quality_thresholds bill_committee_report_multiplicity_allowlist --full-refresh \
      || { seed_status=$?; cancel_dbt_backends "$dbt_app_name"; status=1; }

  dbt_run_status=0
  dbt_test_status=0
  # Materialize all category-tagged models and the dependency closure of the
  # quality marts.  Running this separately from tests prevents the broad
  # schema-test graph from exhausting the refresh timeout before the gate.
  PGAPPNAME="$dbt_app_name" \
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${BICAM_SUPPLEMENTAL_RUN_STATEMENT_TIMEOUT_MS:-1740000}" \
    timeout --signal=TERM --kill-after=30s 1800s \
    uv run dbt run --no-partial-parse --project-dir dbt_project \
      --select int_raw_response_versions int_raw_responses__current \
        tag:amendments tag:bills tag:committee_meetings tag:committee_prints \
        tag:committee_reports tag:committees tag:hearings tag:members \
        tag:nominations tag:treaties \
        +fct_reconciliation_metrics +int_committee_report_candidates \
        +fct_source_gap_queue bills_versions bills_version_links \
        +bridge_hearings__committee_meetings \
        +bridge_bills__committee_reports \
        +bridge_members__legislation \
        +bridge_committees__bills \
        +fct_congress_internal_reciprocity_metrics \
        +fct_govinfo_data_quality_metrics \
        +fct_govinfo_committee_id_exceptions \
        +fct_govinfo_member_reference_identity_exceptions \
        +fct_govinfo_member_identity_candidates \
        +fct_govinfo_nested_identity_exceptions \
        +int_govinfo_multi_legislator_candidates \
        bridge_govinfo__member_components \
        stg_govinfo__member_references \
        +fct_committee_print_matches \
        +fct_committee_print_reconciliation_metrics \
        bridge_govinfo__committee_print_packages \
      || { dbt_run_status=$?; cancel_dbt_backends "$dbt_app_name"; status=1; }

  # Keep the same auditable reconciliation/identity corpus used by the
  # incremental lane.  Full schema sweeps remain available to CI and the
  # read-only audit service.
  if (( dbt_run_status == 0 )); then
    PGAPPNAME="$dbt_app_name" \
      PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${BICAM_SUPPLEMENTAL_TEST_STATEMENT_TIMEOUT_MS:-840000}" \
      timeout --signal=TERM --kill-after=30s 900s \
      uv run dbt test --no-partial-parse --project-dir dbt_project \
        --select congress_internal_reciprocity_has_no_duplicate_edges \
          congress_internal_reciprocity_status_is_consistent \
          child_full_snapshot_precedes_incremental_empty_response \
          incremental_child_windows_are_accumulated \
          bill_report_part_asymmetries_are_classified \
          nomination_child_part_ids_preserve_parts \
          nominations_nominees_preserve_source_observations \
          nominations_nominees_preserve_duplicate_ordinals \
          nominations_nominees_have_no_repeated_public_rows \
          bills_titles_have_one_row_per_semantic_key \
          bill_cosponsors_have_one_current_observation \
          congress_member_amendment_reciprocity_has_both_directions \
          congress_committee_report_reciprocity_has_both_directions \
          govinfo_committee_id_overrides_target_observed \
          fct_govinfo_member_identity_candidates \
          govinfo_multi_legislator_components_are_preserved \
          govinfo_multi_legislator_report_congress_precedence \
          govinfo_nested_identity_audit_covers_payloads \
          govinfo_committee_id_map_covers_nested_authority_ids \
          govinfo_report_bill_member_matches_are_evidence_backed \
          govinfo_member_reference_identity_exceptions_are_unresolved \
          govinfo_leading_conjunction_member_refs_are_resolved \
          committee_bill_subcommittee_asymmetries_are_classified \
          govinfo_member_reference_source_ids_are_canonical \
          govinfo_member_component_overrides_match_candidates \
          govinfo_component_override_bioguide_ids_are_valid \
          govinfo_member_candidate_chamber_is_consistent \
          govinfo_nested_identity_context_is_consistent \
          congress_internal_union_preserves_edge_directions \
          reconciliation_unexcused_records_never_pass \
          reconciliation_evidence_source_gaps_expire \
          reconciliation_title_corroboration_meets_threshold \
          govinfo_hearing_meeting_evidence_is_preserved \
          hearing_committee_meeting_bridge_preserves_source_links \
          hearing_committee_meeting_bridge_has_valid_ids \
          committee_report_bill_bridge_preserves_union \
          bill_committee_report_multiplicity_allowlist_is_current \
          committee_print_bridge_preserves_every_package \
          committee_print_canonical_ids_are_valid \
          committee_print_collection_filter_is_explicit \
          committee_print_reconciliation_counts_are_consistent \
          committee_print_text_formats_are_preserved \
          committee_print_direct_renditions_are_preserved \
          govinfo_member_overrides_match_congress_terms \
          committee_report_bill_union_preserves_directions \
          committee_report_congress_union_includes_bill_side \
          committee_report_candidates_one_per_canonical \
          committee_reports_public_union_preserves_bill_side \
          member_legislation_bridge_preserves_union \
          committee_bill_bridge_preserves_union \
          bill_detail_duplicate_ids_are_historical \
          congress_root_source_errors_have_identity_fallback \
          raw_child_parent_lineage_is_unique \
          raw_version_parent_lineage_propagates \
          bill_text_parent_lineage_propagates \
          committee_report_text_parent_lineage_propagates \
          congress_bill_number_macro_normalizes_half \
          congress_bill_numbers_have_no_unicode_fraction_suffix \
          bills_version_links_have_version_evidence \
          bills_version_links_title_codes_match_source \
          bills_version_links_exclude_summary_codes \
        || { dbt_test_status=$?; cancel_dbt_backends "$dbt_app_name"; status=1; }
  fi
  echo "- dbt model status: $dbt_run_status"
  echo "- dbt targeted-test status: $dbt_test_status"

  quality_gate_status=0
  timeout --signal=TERM --kill-after=30s 900s \
    .venv/bin/python scripts/check_quality_gates.py \
      --max-open-exceptions 0 --max-unsatisfied-fetches 0 \
      --max-missing-full-scope-partitions 0 \
      --max-missing-root-items 0 --max-missing-child-items 0 || quality_gate_status=$?
  echo "quality_gate_status=$quality_gate_status"
  if (( quality_gate_status != 0 )); then
    status=1
  fi
  if (( status == 0 && dbt_run_status == 0 && dbt_test_status == 0 && quality_gate_status == 0 )); then
    echo "combined_release_gate=PASS"
  else
    echo "combined_release_gate=FAIL"
  fi
} >"$log" 2>&1

{
  echo '# Congress supplemental six-hour checkpoint'
  echo
  echo "- started: $started_at"
  echo "- finished: $(date -u --iso-8601=seconds)"
  echo "- status: $status"
  echo '- refresh scope: the 11 selected Congress.gov families only.'
  echo
  echo '```text'
  tail -n 100 "$log" 2>/dev/null || true
  echo '```'
} >"$checkpoint"
ln -sfn "$(basename "$checkpoint")" "$attempt_latest"
if (( status == 0 )); then
  ln -sfn "$(basename "$checkpoint")" "$latest"
fi
exit "$status"
