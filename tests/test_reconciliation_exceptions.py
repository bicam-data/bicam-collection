from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_missing_congress_committee_reports_construct_targeted_probe_endpoints() -> None:
    model = (
        PROJECT_ROOT
        / "dbt_project/models/marts/reconciliation/fct_reconciliation_exceptions.sql"
    ).read_text()

    assert "entity = 'committee_report'" in model
    assert "missing_source = 'congress'" in model
    assert "https://api.congress.gov/v3/committee-report/" in model
    assert "^([a-z]+)([0-9]+)-([0-9]+)-([0-9]+)e?$" in model
    assert "expected_missing_endpoint_url" in model
