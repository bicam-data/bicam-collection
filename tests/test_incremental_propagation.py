from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_congress_incremental_refresh_propagates_raw_models_before_quality_build() -> None:
    wrapper = (PROJECT_ROOT.parent / "scripts" / "run_congress_incremental_refresh.sh").read_text()
    propagation = "uv run dbt run --no-partial-parse --project-dir dbt_project"
    assert propagation in wrapper
    assert "int_raw_response_versions int_raw_responses__current" in wrapper
    assert "raw intermediate propagation status" in wrapper
    assert "propagation_status" in wrapper


def _resolved_window_ids(rows: list[dict[str, object]]) -> set[str]:
    """Reference semantics for the SQL child-window resolver.

    This deliberately models two non-overlapping deltas, an empty poll, and a
    later complete snapshot. It is kept independent of dbt/Postgres so the
    regression remains visible in the fast unit suite.
    """
    complete = [row for row in rows if not row["incremental"]]
    latest_complete = max(
        complete,
        key=lambda row: (row["fetched_at"], row["id"]),
        default=None,
    )
    selected: set[str] = set()
    if latest_complete is not None:
        selected.add(str(latest_complete["id"]))
    for row in rows:
        if not row["incremental"] or not row["items"]:
            continue
        if latest_complete is None or row["fetched_at"] >= latest_complete["fetched_at"]:
            selected.add(str(row["id"]))
    return selected


def test_incremental_windows_accumulate_until_a_new_complete_snapshot() -> None:
    rows = [
        {"id": "full-a", "incremental": False, "items": True, "fetched_at": 1},
        {"id": "delta-b", "incremental": True, "items": True, "fetched_at": 2},
        {"id": "delta-c", "incremental": True, "items": True, "fetched_at": 3},
        {"id": "empty-d", "incremental": True, "items": False, "fetched_at": 4},
        {"id": "full-e", "incremental": False, "items": True, "fetched_at": 5},
        {"id": "delta-f", "incremental": True, "items": True, "fetched_at": 6},
    ]

    assert _resolved_window_ids(rows[:4]) == {"full-a", "delta-b", "delta-c"}
    assert _resolved_window_ids(rows) == {"full-e", "delta-f"}


def test_empty_complete_snapshot_replaces_prior_membership_but_empty_delta_does_not() -> None:
    rows = [
        {"id": "full-a", "incremental": False, "items": True, "fetched_at": 1},
        {"id": "empty-delta", "incremental": True, "items": False, "fetched_at": 2},
        {"id": "empty-full", "incremental": False, "items": False, "fetched_at": 3},
        {"id": "delta-after-empty", "incremental": True, "items": True, "fetched_at": 4},
    ]

    assert _resolved_window_ids(rows[:2]) == {"full-a"}
    assert _resolved_window_ids(rows[:3]) == {"empty-full"}
    assert _resolved_window_ids(rows) == {"empty-full", "delta-after-empty"}


def test_incremental_window_regression_is_encoded_in_macro_and_dbt_test() -> None:
    macro = (PROJECT_ROOT / "dbt_project/macros/resolve_child.sql").read_text()
    selection = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/raw/int_congress_child_response_selection.sql"
    ).read_text()
    selections = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/raw/int_congress_child_response_selections.sql"
    ).read_text()
    data_test = (PROJECT_ROOT / "dbt_project/tests/incremental_child_windows_are_accumulated.sql").read_text()
    assert "annotated.fetched_at >= complete.fetched_at" in selections
    assert "order by annotated.fetched_at desc" in selections
    assert "order by annotated.has_array_items desc" not in selections
    assert "annotated.has_array_field" in selections
    assert "child_json_nodes" in selection
    assert "nested_array_field_count" in selection
    assert "annotated.array_field_count > 0" in selections
    assert "annotated.array_item_count = 0" not in macro
    assert "int_congress_child_response_selection" in macro
    assert "int_congress_child_response_selections" in macro
    assert "from {{ ref('int_raw_responses__current') }}" not in macro
    assert "child_json_nodes" not in macro
    endpoint_macro = (
        PROJECT_ROOT / "dbt_project/macros/congress_child_logical_endpoint.sql"
    ).read_text()
    assert "congress_child_logical_endpoint" in selection
    assert "format|limit|offset|fromDateTime|toDateTime|api_key" in endpoint_macro
    assert "class_rank" not in macro
    assert "expected_deltas" in data_test


def test_child_selection_reads_immutable_versions_and_keeps_version_identity() -> None:
    selection = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/raw/int_congress_child_response_selection.sql"
    ).read_text()
    selections = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/raw/int_congress_child_response_selections.sql"
    ).read_text()
    schema = (PROJECT_ROOT / "dbt_project/models/intermediate/raw/schema.yml").read_text()

    assert "from {{ ref('int_raw_response_versions') }} as versions" in selection
    assert "current_response.raw_response_version_id = versions.raw_response_version_id" in selection
    assert "versions.last_seen_at, versions.first_seen_at" in selection
    assert '"raw_response_version_id"], "unique": true' in selection
    assert "complete.raw_response_version_id = annotated.raw_response_version_id" in selections
    assert "name: raw_response_version_id" in schema


def test_transport_child_facts_are_incremental_and_grouped_by_version() -> None:
    """Repeated propagation must not rescan or cross-contaminate old payloads."""

    selection = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/raw/int_congress_child_response_selection.sql"
    ).read_text()

    assert 'materialized="incremental"' in selection
    assert 'unique_key="raw_response_version_id"' in selection
    assert 'incremental_strategy="delete+insert"' in selection
    assert "not exists" in selection
    assert "responses.raw_response_version_id" in selection
    assert "nodes.raw_response_version_id" in selection
    assert "group by responses.raw_response_version_id" in selection
    assert "group by nodes.raw_response_version_id" in selection
    assert "top_array_stats.raw_response_version_id = responses.raw_response_version_id" in selection
    assert "left join nested_array_stats using (raw_response_version_id)" in selection


def test_current_raw_projection_tracks_repeated_fetch_evidence() -> None:
    current = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/raw/int_raw_responses__current.sql"
    ).read_text()

    assert "existing.fetched_at is not distinct from current_response.fetched_at" in current
    assert (
        "existing.latest_crawl_run_id is not distinct from current_response.latest_crawl_run_id"
        in current
    )


def test_recorded_vote_public_indexes_are_scoped_to_the_action() -> None:
    for relative_path in (
        "dbt_project/models/marts/bicam/bills_actions_recorded_votes.sql",
        "dbt_project/models/marts/bicam/amendments/amendments_actions_recorded_votes.sql",
    ):
        model = (PROJECT_ROOT / relative_path).read_text()
        assert (
            "['action_id', 'chamber', 'congress', 'session', 'roll_number'], 'unique': true"
            in model
        )


def test_resolved_children_retain_version_and_collection_lineage() -> None:
    macro = (PROJECT_ROOT / "dbt_project/macros/resolve_child.sql").read_text()
    bill_observations = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/congress/int_congress_bill_cosponsor_observations.sql"
    ).read_text()
    nominee_observations = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/congress/int_congress_nominee_observations.sql"
    ).read_text()

    assert "child.raw_response_version_id" in macro
    assert "child.latest_crawl_run_id" in macro
    assert "logical_collection" in macro
    assert "source_raw_response_version_id" in bill_observations
    assert "source_crawl_run_id" in bill_observations
    assert "source_raw_response_version_id" in nominee_observations
    assert "source_crawl_run_id" in nominee_observations


def test_member_amendment_reciprocity_uses_resolved_cosponsor_children() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/reconciliation/"
        / "int_congress_member_amendment_relationship_edges.sql"
    ).read_text()

    assert "int_congress_amendment_cosponsor_observations" in model
    assert "from {{ ref('int_raw_responses__current') }} as raw" not in model

    observations = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/congress/"
        / "int_congress_amendment_cosponsor_observations.sql"
    ).read_text()
    assert "source_raw_response_version_id" in observations
    assert "source_generation_expected_count" in observations
    assert "resolve_child('amendment_detail', 'amendment_cosponsors', 'amendments')" in observations


def test_amendment_cosponsor_staging_consumes_resolved_observations() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/staging/congress/"
        / "stg_congress__amendment_cosponsors.sql"
    ).read_text()

    assert "int_congress_amendment_cosponsor_observations" in model
    assert "from {{ ref('int_raw_responses__current') }}" not in model
    assert "partition by observations.amendment_id, observations.bioguide_id" in model


def test_member_legislation_staging_consumes_shared_observations() -> None:
    for name in (
        "stg_congress__member_sponsored_legislation.sql",
        "stg_congress__member_cosponsored_legislation.sql",
    ):
        model = (
            PROJECT_ROOT / "dbt_project/models/staging/congress" / name
        ).read_text()
        assert "int_congress_member_legislation_observations" in model
        assert "resolve_child('member_detail'" not in model


def test_bill_subject_and_summary_staging_consume_observations() -> None:
    for name, observation in (
        (
            "stg_congress__bill_subjects.sql",
            "int_congress_bill_subject_observations",
        ),
        (
            "stg_congress__bill_summaries.sql",
            "int_congress_bill_summary_observations",
        ),
    ):
        model = (
            PROJECT_ROOT / "dbt_project/models/staging/congress" / name
        ).read_text()
        assert observation in model
        assert "resolve_child('bill_detail'" not in model


def test_bill_title_staging_consumes_observations() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/staging/congress/stg_congress__bill_titles.sql"
    ).read_text()

    assert "int_congress_bill_title_observations" in model
    assert "resolve_child('bill_detail'" not in model


def test_bill_title_observations_use_selected_parent_payloads() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/congress/int_congress_bill_title_observations.sql"
    ).read_text()

    assert "int_congress_child_response_selections" in model
    assert "int_raw_responses__current" in model
    assert "stg_congress__bill_detail" not in model


def test_bill_action_staging_consumes_shared_observations() -> None:
    for name in (
        "stg_congress__bill_actions.sql",
        "stg_congress__bill_actions_committees.sql",
        "stg_congress__bill_actions_recordedvotes.sql",
    ):
        model = (
            PROJECT_ROOT / "dbt_project/models/staging/congress" / name
        ).read_text()
        assert "int_congress_bill_action_observations" in model
        assert "resolve_child('bill_detail'" not in model


def test_amendment_action_staging_consumes_shared_observations() -> None:
    for name in (
        "stg_congress__amendment_actions.sql",
        "stg_congress__amendment_actions_committees.sql",
        "stg_congress__amendment_actions_recordedvotes.sql",
    ):
        model = (
            PROJECT_ROOT / "dbt_project/models/staging/congress" / name
        ).read_text()
        assert "int_congress_amendment_action_observations" in model
        assert "resolve_child('amendment_detail'" not in model


def test_treaty_action_staging_consumes_resolved_observations() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/staging/congress/stg_congress__treaty_actions.sql"
    ).read_text()

    assert "int_congress_treaty_action_observations" in model
    assert "resolve_child('treaty_detail'" not in model

    observations = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/congress/"
        / "int_congress_treaty_action_observations.sql"
    ).read_text()
    assert "source_raw_response_version_id" in observations
    assert "source_generation_expected_count" in observations
    assert "resolve_child('treaty_detail', 'treaty_actions', 'treaties')" in observations


def test_treaty_committee_staging_consumes_shared_observations() -> None:
    for name in (
        "stg_congress__treaty_committees.sql",
        "stg_congress__treaty_committeeactivities.sql",
    ):
        model = (
            PROJECT_ROOT / "dbt_project/models/staging/congress" / name
        ).read_text()
        assert "int_congress_treaty_committee_observations" in model
        assert "resolve_child('treaty_detail'" not in model

    observations = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/congress/"
        / "int_congress_treaty_committee_observations.sql"
    ).read_text()
    assert "source_raw_response_version_id" in observations
    assert "source_generation_expected_count" in observations
    assert "resolve_child('treaty_detail', 'treaty_committees', 'treaties')" in observations


def test_committee_membership_edge_staging_consumes_resolved_observations() -> None:
    for name, observation in (
        (
            "stg_congress__committee_communications.sql",
            "int_congress_committee_communication_observations",
        ),
        (
            "stg_congress__committee_nominations.sql",
            "int_congress_committee_nomination_observations",
        ),
    ):
        model = (
            PROJECT_ROOT / "dbt_project/models/staging/congress" / name
        ).read_text()
        assert observation in model
        assert "resolve_child('committee_detail'" not in model


def test_nominee_staging_consumes_resolved_observations() -> None:
    for name in (
        "stg_congress__nomination_nominees.sql",
        "stg_congress__nomination_nominee_details.sql",
    ):
        model = (
            PROJECT_ROOT / "dbt_project/models/staging/congress" / name
        ).read_text()
        assert "int_congress_nominee_observations" in model
        assert "resolve_child('nomination_detail'" not in model


def test_nomination_action_staging_consumes_shared_observations() -> None:
    for name in (
        "stg_congress__nomination_actions.sql",
        "stg_congress__nomination_actions_committees.sql",
    ):
        model = (
            PROJECT_ROOT / "dbt_project/models/staging/congress" / name
        ).read_text()
        assert "int_congress_nomination_action_observations" in model
        assert "resolve_child('nomination_detail'" not in model

    observations = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/congress/"
        / "int_congress_nomination_action_observations.sql"
    ).read_text()
    assert "source_raw_response_version_id" in observations
    assert "source_generation_expected_count" in observations
    assert "resolve_child('nomination_detail', 'nomination_actions', 'nominations')" in observations


def test_nomination_hearing_and_activity_staging_consume_observations() -> None:
    for name, observation in (
        (
            "stg_congress__nomination_hearings.sql",
            "int_congress_nomination_hearing_observations",
        ),
        (
            "stg_congress__nomination_committeeactivities.sql",
            "int_congress_nomination_committee_activity_observations",
        ),
    ):
        model = (
            PROJECT_ROOT / "dbt_project/models/staging/congress" / name
        ).read_text()
        assert observation in model
        assert "resolve_child('nomination_detail'" not in model


def test_amendment_amendment_staging_consumes_resolved_observations() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/staging/congress/"
        / "stg_congress__amendment_amendments_to_amendment.sql"
    ).read_text()
    assert "int_congress_amendment_amendment_observations" in model
    assert "resolve_child('amendment_detail'" not in model


def test_committee_report_staging_consumes_shared_observations() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/staging/congress/stg_congress__committee_reports.sql"
    ).read_text()

    assert "int_congress_committee_report_observations" in model
    assert "resolve_child('committee_detail'" not in model


def test_bill_related_bill_staging_uses_latest_selected_source_observation() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/staging/congress/stg_congress__bill_relatedbills.sql"
    ).read_text()

    assert "int_congress_bill_related_bill_observations" in model
    assert "child_fetched_at" not in model
    assert "raw_id as source_raw_id" not in model
    assert "order by source_fetched_at desc nulls last" in model
    assert "relationship_identified_by asc nulls last" in model

    observations = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/congress/int_congress_bill_related_bill_observations.sql"
    ).read_text()
    assert "resolve_child('bill_detail', 'bill_relatedbills', 'bills')" in observations
    assert "source_raw_response_version_id" in observations
    assert "relationship_ordinal" in observations


def test_bill_amendment_and_committee_staging_consume_lineage_observations() -> None:
    for staging_name, observation_name in (
        (
            "stg_congress__bill_amendments.sql",
            "int_congress_bill_amendment_observations",
        ),
        (
            "stg_congress__bill_committees.sql",
            "int_congress_bill_committee_observations",
        ),
        (
            "stg_congress__committee_bills.sql",
            "int_congress_committee_bill_observations",
        ),
    ):
        staging = (PROJECT_ROOT / "dbt_project/models/staging/congress" / staging_name).read_text()
        assert observation_name in staging
        assert "resolve_child('bill_detail'" not in staging


def test_bill_relationship_observations_retain_resolver_lineage() -> None:
    for relative_path, resolver in (
        (
            "dbt_project/models/intermediate/congress/int_congress_bill_amendment_observations.sql",
            "resolve_child('bill_detail'",
        ),
        (
            "dbt_project/models/intermediate/congress/int_congress_bill_committee_observations.sql",
            "resolve_child('bill_detail'",
        ),
        (
            "dbt_project/models/intermediate/congress/int_congress_committee_bill_observations.sql",
            "resolve_child('committee_detail'",
        ),
    ):
        model = (PROJECT_ROOT / relative_path).read_text()
        assert "source_raw_response_version_id" in model
        assert "source_generation_expected_count" in model
        assert "source_ordinal" in model
        assert resolver in model


def test_bill_text_versions_use_resolved_child_windows() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/raw/int_congress_bill_text_versions.sql"
    ).read_text()

    assert "int_congress_child_response_selections" in model
    assert "selected_text_responses as materialized" in model
    assert "join {{ ref('int_raw_responses__current') }} as parent" in model
    assert "text_response.parent_id as bill_detail_raw_id" in model
    assert "stg_congress__bill_detail" not in model
    assert "bill_number_text ~ '^[0-9]+½$'" in model


def test_committee_report_text_formats_use_resolved_child_windows() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/raw/"
        / "int_congress_committee_report_text_formats.sql"
    ).read_text()

    assert "resolve_child('committee_report_detail', 'committee_report_texts', 'committee-reports')" in model
    assert "from {{ ref('int_raw_responses__current') }} as child" not in model
    assert "text_responses.parent_raw_id" in model
    assert "report_detail_raw_id" in model


def _generation_is_complete(pages: list[dict[str, int]]) -> bool:
    """Reference the resolver's contiguous-page completeness proof."""
    if not pages:
        return False
    ordered = sorted(pages, key=lambda row: row["offset"])
    expected = {row["expected"] for row in ordered}
    if len(expected) != 1 or ordered[0]["offset"] != 0:
        return False
    previous_end = 0
    observed = 0
    for page in ordered:
        if page["offset"] != previous_end:
            return False
        previous_end = page["offset"] + page["items"]
        observed += page["items"]
    return observed == ordered[0]["expected"] == previous_end


def test_paged_generation_requires_contiguous_offsets_and_api_total() -> None:
    complete = [
        {"offset": 0, "items": 10, "expected": 25},
        {"offset": 10, "items": 10, "expected": 25},
        {"offset": 20, "items": 5, "expected": 25},
    ]
    gap = [
        {"offset": 0, "items": 10, "expected": 25},
        {"offset": 20, "items": 5, "expected": 25},
    ]
    overlapping = [
        {"offset": 0, "items": 10, "expected": 25},
        {"offset": 9, "items": 10, "expected": 25},
        {"offset": 19, "items": 6, "expected": 25},
    ]

    assert _generation_is_complete(complete)
    assert not _generation_is_complete(gap)
    assert not _generation_is_complete(overlapping)


def test_generation_proof_is_present_in_resolver_and_exposes_fallback_state() -> None:
    macro = (PROJECT_ROOT / "dbt_project/macros/resolve_child.sql").read_text()
    selections = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/raw/int_congress_child_response_selections.sql"
    ).read_text()

    assert "latest_complete_full_generations" in selections
    assert "pages_are_contiguous" in selections
    assert "observed_generation_count = summaries.maximum_expected_count" in selections
    assert "selected.is_complete_generation" in selections
    assert "selected.generation_expected_count" in selections
    assert "int_congress_child_response_selections" in macro
    assert "not (annotated.response ? 'pagination')" in selections
    assert "annotated.array_field_count > 0" in selections


def test_selected_child_state_has_generation_and_latest_page_indexes() -> None:
    selections = (
        PROJECT_ROOT
        / "dbt_project/models/intermediate/raw/int_congress_child_response_selections.sql"
    ).read_text()

    # These are the two expensive downstream access paths: generation-window
    # proofs and latest-page fallback selection. Keep them explicit so the
    # production-sized materialization does not repeatedly sort all payloads.
    assert '"latest_crawl_run_id"' in selections
    assert '"is_incremental", "page_offset", "raw_id"' in selections
    assert '"category", "node_type", "parent_id"' in selections
    assert '"logical_endpoint", "child_fetched_at", "raw_id"' in selections


def test_child_expansions_rebuild_so_complete_replacements_can_remove_rows() -> None:
    """Incremental text expansions would retain rows retired by an empty full child."""

    for relative_path in (
        "dbt_project/models/intermediate/raw/int_congress_bill_text_versions.sql",
        "dbt_project/models/intermediate/raw/int_congress_bill_text_formats.sql",
        "dbt_project/models/intermediate/raw/int_congress_committee_report_records.sql",
        "dbt_project/models/intermediate/raw/int_congress_committee_report_text_formats.sql",
    ):
        model = (PROJECT_ROOT / relative_path).read_text()
        assert 'materialized="table"' in model
        assert "is_incremental()" not in model
