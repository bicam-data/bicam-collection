from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DECLARED_FAMILIES = (
    "member_bill_sponsor",
    "member_bill_cosponsor",
    "member_amendment_sponsor",
    "member_amendment_cosponsor",
    "committee_bill",
    "bill_amendment",
    "bill_committee_report",
    "committee_nomination",
    "committee_committee_report",
    "committee_meeting_hearing",
    "treaty_committee_report",
)


def test_reciprocity_metrics_declare_every_audited_relationship_family() -> None:
    metrics = (
        PROJECT_ROOT
        / "dbt_project/models/marts/operations/fct_congress_internal_reciprocity_metrics.sql"
    ).read_text()
    coverage_test = (
        PROJECT_ROOT
        / "dbt_project/tests/congress_internal_reciprocity_metrics_cover_declared_families.sql"
    ).read_text()

    for family in DECLARED_FAMILIES:
        assert f"('{family}')" in metrics
        assert f"('{family}')" in coverage_test


def test_reciprocity_metrics_expose_explicit_release_acceptance_policy() -> None:
    metrics = (
        PROJECT_ROOT
        / "dbt_project/models/marts/operations/fct_congress_internal_reciprocity_metrics.sql"
    ).read_text()
    acceptance_test = (
        PROJECT_ROOT
        / "dbt_project/tests/congress_internal_reciprocity_release_acceptance_is_fail_closed.sql"
    ).read_text()

    assert "release_acceptance_status" in metrics
    assert "accepted_with_source_boundaries" in metrics
    assert "reject_contradiction" in acceptance_test
    assert "reject_unclassified" in acceptance_test
