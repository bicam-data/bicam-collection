from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.report_source_gap_queue import (
    ITEM_SQL,
    SUMMARY_SQL,
    _send_pushover,
    actionable_row_count,
    render_markdown,
)


def test_render_markdown_contains_summary_and_actionable_item() -> None:
    report = {
        "generated_at": "2026-09-01T00:00:00+00:00",
        "total_rows": 1,
        "summary": {"open|high": 1},
        "items": [
            {
                "entity": "bill",
                "canonical_id": "hr1-119",
                "missing_source": "govinfo",
                "priority": "high",
                "next_action": "fetch_missing_source",
                "endpoint_url": "https://api.govinfo.gov/packages/BILLS-119hr1ih/summary",
            }
        ],
    }
    rendered = render_markdown(report)
    assert "BICAM source-gap queue" in rendered
    assert "hr1-119" in rendered
    assert "fetch_missing_source" in rendered


def test_render_markdown_supports_incomplete_child_state_queue_items() -> None:
    rendered = render_markdown(
        {
            "generated_at": "2026-09-06T04:00:00+00:00",
            "total_rows": 1,
            "summary": {"open|low": 1},
            "items": [
                {
                    "entity": "child_state",
                    "canonical_id": "committee_print_texts|https://api.congress.gov/v3/committee-print/118/house/47777/text",
                    "missing_source": "congress",
                    "priority": "low",
                    "next_action": "reprobe",
                    "endpoint_url": "https://api.congress.gov/v3/committee-print/118/house/47777/text?format=json",
                    "latest_fetch_outcome": "incomplete",
                }
            ],
        }
    )
    assert "child_state" in rendered
    assert "reprobe" in rendered
    assert "committee_print_texts" in rendered


def test_pushover_is_inert_without_explicit_credentials(monkeypatch) -> None:
    monkeypatch.delenv("PUSHOVER_TOKEN", raising=False)
    monkeypatch.delenv("PUSHOVER_USER", raising=False)
    try:
        _send_pushover("test")
    except ValueError as error:
        assert "requires PUSHOVER_TOKEN" in str(error)
    else:
        raise AssertionError("missing Pushover credentials should not send")


def test_pushover_alert_count_excludes_deferred_source_boundaries() -> None:
    assert actionable_row_count(
        {"summary": {"deferred_source_gap|low": 100, "open|high": 2, "due_for_reprobe|normal": 3}}
    ) == 5
    assert actionable_row_count({"summary": {"deferred_source_gap|low": 100}}) == 0


def test_queue_report_recomputes_expiry_sensitive_status_at_read_time() -> None:
    assert "current_queue_status" in SUMMARY_SQL
    assert "expires_at <= now()" in SUMMARY_SQL
    assert "current_next_action" in ITEM_SQL
    assert "current_next_action_at" in ITEM_SQL
