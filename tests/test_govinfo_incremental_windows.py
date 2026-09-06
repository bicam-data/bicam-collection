from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HOST_WRAPPER = PROJECT_ROOT.parent / "scripts" / "run_govinfo_incremental_windows.sh"
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_govinfo_incremental_windows.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("run_govinfo_incremental_windows", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_all_four_govinfo_collections_share_the_window_frontier() -> None:
    script = _load_module()
    assert [collection.category for collection in script.COLLECTIONS] == [
        "govinfo-bills",
        "govinfo-committee-reports",
        "govinfo-committee-prints",
        "govinfo-hearings",
    ]


def test_timestamp_round_trip_is_utc_and_second_precision() -> None:
    script = _load_module()
    parsed = script.parse_timestamp("2026-08-24T04:29:45.987654+00:00")
    assert parsed == datetime(2026, 8, 24, 4, 29, 45, tzinfo=timezone.utc)
    assert script.format_timestamp(parsed) == "2026-08-24T04:29:45Z"


def test_next_window_is_bounded_by_size_and_cutoff() -> None:
    script = _load_module()
    frontier = script.parse_timestamp("2026-08-24T00:00:00Z")
    cutoff = script.parse_timestamp("2026-08-24T13:00:00Z")
    assert script.next_window(
        frontier=frontier,
        cutoff=cutoff,
        window_hours=6,
        minimum_window_minutes=5,
    ) == (frontier, script.parse_timestamp("2026-08-24T06:00:00Z"))

    short_frontier = script.parse_timestamp("2026-08-24T12:58:00Z")
    assert (
        script.next_window(
            frontier=short_frontier,
            cutoff=cutoff,
            window_hours=6,
            minimum_window_minutes=5,
        )
        is None
    )


def test_crawl_command_never_uses_a_page_budget() -> None:
    script = _load_module()
    command = script.build_crawl_command(
        project_root=PROJECT_ROOT,
        collection=script.COLLECTIONS[0],
        window_start=script.parse_timestamp("2026-08-24T00:00:00Z"),
        window_end=script.parse_timestamp("2026-08-24T06:00:00Z"),
        concurrency=3,
        page_size=100,
        request_interval=0.25,
        key_wait_timeout=600,
    )
    assert "--max-pages" not in command
    assert command[command.index("--category") + 1] == "govinfo-bills"
    assert command[command.index("--mode") + 1] == "full"
    assert command[command.index("--from-datetime") + 1] == "2026-08-24T00:00:00Z"
    assert command[command.index("--to-datetime") + 1] == "2026-08-24T06:00:00Z"


def test_subprocess_output_redacts_api_credentials() -> None:
    script = _load_module()
    assert (
        script.redact_output("https://api.govinfo.gov/x?api_key=secret&pageSize=100")
        == "https://api.govinfo.gov/x?api_key=[REDACTED]&pageSize=100"
    )


def test_hourly_wrapper_probes_new_govinfo_hearing_counterparts() -> None:
    wrapper = HOST_WRAPPER.read_text()
    assert "int_raw_response_versions int_raw_responses__current" in wrapper
    assert "backfill_congress_govinfo_hearings.py" in wrapper
    assert "BICAM_GOVINFO_HEARING_COUNTERPART_LIMIT" in wrapper
    assert "BICAM_GOVINFO_HEARING_COUNTERPART_ACTIVE_KEY_BUDGET" in wrapper
    assert "resolve_current_congress.py" in wrapper
    assert 'BICAM_GOVINFO_HEARING_COUNTERPART_CONGRESS_END:-$current_congress' in wrapper


def test_reconciliation_queue_selectors_include_child_state_ancestors() -> None:
    """The queue depends on child-state gaps and must rebuild that parent first."""

    govinfo_wrapper = HOST_WRAPPER.read_text()
    congress_supplemental = HOST_WRAPPER.parent / "run_congress_supplemental_refresh.sh"
    congress_incremental = HOST_WRAPPER.parent / "run_congress_incremental_refresh.sh"

    assert "+fct_source_gap_queue bills_versions bills_version_links" in govinfo_wrapper
    assert "+fct_source_gap_queue bills_versions bills_version_links" in congress_supplemental.read_text()
    assert "  +fct_source_gap_queue\n" in congress_incremental.read_text()


def test_hourly_reconciliation_refreshes_report_root_and_edges_together() -> None:
    """The candidate FK cannot see a mixed report root/edge generation."""

    wrapper = HOST_WRAPPER.read_text()
    assert "bills bills_metadata committee_reports committee_reports_associated_bills" in wrapper
    assert "committee_reports_public_union_preserves_bill_side" in wrapper
    assert "committee_reports_associated_bills_cover_internal_union" in wrapper


def test_hourly_gap_lane_probes_missing_congress_committee_reports() -> None:
    """Open GovInfo-only report gaps need bounded Congress-side evidence."""

    wrapper = HOST_WRAPPER.read_text()
    assert "backfill_congress_committee_reports.py" in wrapper
    assert "BICAM_GOVINFO_CONGRESS_REPORT_PROBE_LIMIT" in wrapper
    assert "congress_committee_report_probe_status" in wrapper
    assert "Congress committee-report probe status:" in wrapper


def test_govinfo_timeout_cleanup_retries_exact_application_name() -> None:
    """A timed-out reconciliation must not leave uniquely tagged writers behind."""

    wrapper = HOST_WRAPPER.read_text()
    assert "cleanup_dbt_backends()" in wrapper
    assert "trap cleanup_dbt_backends EXIT" in wrapper
    assert "trap 'exit 143' TERM" in wrapper
    assert "trap 'exit 130' INT" in wrapper
    assert "cancel_attempt in 1 2" in wrapper
    assert "application_name = :'app_name'" in wrapper
    assert "pg_terminate_backend(pid)" in wrapper
