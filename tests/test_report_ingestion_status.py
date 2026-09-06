from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "report_ingestion_status.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("report_ingestion_status", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_child_backlog_parser_reads_bill_and_amendment_rows(tmp_path: Path) -> None:
    script = _load_module()
    audit = tmp_path / "audit.txt"
    audit.write_text(
        "Wed Aug 12 01:47:58 AM UTC 2026\n"
        "bills bill_detail -> bill_actions expected=10 fetched=3 missing=7\n"
        "amendments amendment_detail -> amendment_texts expected=8 fetched=2 missing=6\n"
        "committee-meetings committee_meeting_detail -> hearings expected=4 fetched=3 missing=1\n"
        "child_coverage_missing_total=14\n",
        encoding="utf-8",
    )

    assert script._load_child_backlog(audit) == (13, 14, "Wed Aug 12 01:47:58 AM UTC 2026")


def test_child_backlog_parser_normalizes_checkpoint_label(tmp_path: Path) -> None:
    script = _load_module()
    audit = tmp_path / "child-coverage-audit-latest.txt"
    audit.write_text(
        "Child-coverage audit checkpoint: 2026-08-28 08:34 UTC\n"
        "bills bill_detail -> bill_actions expected=10 fetched=10 source_gaps=0 missing=0\n"
        "child_coverage_expected_total=10\n"
        "child_coverage_missing_total=0\n",
        encoding="utf-8",
    )

    assert script._load_child_backlog(audit) == (0, 10, "2026-08-28 08:34 UTC")


def test_newest_child_audit_prefers_standalone_checkpoint(tmp_path: Path) -> None:
    script = _load_module()
    full_audit = tmp_path / "ingestion-audit-latest.txt"
    child_audit = tmp_path / "child-coverage-audit-latest.txt"
    full_audit.write_text("old\n", encoding="utf-8")
    child_audit.write_text("new\n", encoding="utf-8")
    # Filesystems may have coarse timestamp resolution; explicitly make the
    # standalone checkpoint newer without relying on a sleep.
    import os

    os.utime(full_audit, (1000, 1000))
    os.utime(child_audit, (2000, 2000))
    assert script._newest_child_audit_path(tmp_path) == child_audit


def test_newest_child_audit_finds_timestamped_checkpoint(tmp_path: Path) -> None:
    script = _load_module()
    latest_link_target = tmp_path / "child-coverage-audit-20260828T0311Z.txt"
    latest_link_target.write_text("old\n", encoding="utf-8")
    timestamped = tmp_path / "child-coverage-audit-20260828T0325Z.txt"
    timestamped.write_text("new\n", encoding="utf-8")
    import os

    os.utime(latest_link_target, (1000, 1000))
    os.utime(timestamped, (2000, 2000))
    assert script._newest_child_audit_path(tmp_path) == timestamped


def test_newest_child_audit_includes_supplemental_markdown_checkpoint(tmp_path: Path) -> None:
    script = _load_module()
    legacy = tmp_path / "child-coverage-audit-latest.txt"
    checkpoint = tmp_path / "child-coverage-checkpoint-latest.md"
    legacy.write_text("old\n", encoding="utf-8")
    checkpoint.write_text(
        "Child-coverage audit checkpoint: 2026-08-28 13:08 UTC\n"
        "child_coverage_expected_total=10\n"
        "child_coverage_missing_total=0\n",
        encoding="utf-8",
    )
    import os

    os.utime(legacy, (1000, 1000))
    os.utime(checkpoint, (2000, 2000))
    assert script._newest_child_audit_path(tmp_path) == checkpoint


def test_guard_state_parser_reports_pause_window(tmp_path: Path) -> None:
    script = _load_module()
    state = tmp_path / "guard.txt"
    state.write_text(
        "generated_at=2026-08-12T06:14:05Z\n"
        "state=paused\n"
        "failures_2m=20\n"
        "http_503_2m=2\n"
        "http_429_2m=18\n"
        "fetched_2m=0\n"
        "failure_ratio=1.0000\n",
        encoding="utf-8",
    )

    assert script._load_guard_state(state) == (
        "State: **paused** (last check 2026-08-12T06:14:05Z; "
        "2-minute window: 20 failures, 0 successes, 2 HTTP 503s, 18 HTTP 429s)."
    )


def test_guard_state_parser_reports_quota_cooldown(tmp_path: Path) -> None:
    script = _load_module()
    state = tmp_path / "guard.txt"
    state.write_text(
        "generated_at=2026-08-12T08:09:12Z\n"
        "state=quota_cooldown\n"
        "failures_2m=0\n"
        "http_503_2m=0\n"
        "http_429_2m=0\n"
        "fetched_2m=0\n"
        "failure_ratio=0\n"
        "last_http_429_at=2026-08-12T07:57:23Z\n",
        encoding="utf-8",
    )

    assert script._load_guard_state(state) == (
        "State: **quota_cooldown** (last check 2026-08-12T08:09:12Z; "
        "2-minute window: 0 failures, 0 successes, 0 HTTP 503s, 0 HTTP 429s; "
        "last HTTP 429 2026-08-12T07:57:23Z)."
    )


def test_guard_recent_429_defers_new_probe() -> None:
    script = _load_module()
    now = datetime(2026, 8, 13, 19, 30, tzinfo=timezone.utc)
    assert script._guard_has_recent_429(
        "State: **healthy** (last check 2026-08-13T19:29:00Z; "
        "last HTTP 429 2026-08-13T19:23:49Z).",
        now,
    )
    assert not script._guard_has_recent_429(
        "State: **healthy** (last HTTP 429 2026-08-13T17:00:00Z).",
        now,
    )


def test_govinfo_health_redacts_credentials_and_reports_unauthorized(monkeypatch) -> None:
    script = _load_module()

    class Response:
        status_code = 401

    def fake_get(url, *, params, timeout):
        assert url.endswith("/packages/CRPT-118hrpt1/summary")
        assert params["api_key"] == "secret"
        assert timeout == 10
        return Response()

    monkeypatch.setattr(script.requests, "get", fake_get)
    settings = script.Settings(
        database_url="postgresql://unused",
        env_path=Path("/tmp/no-govinfo-dotenv"),
    )
    # Use the process environment so the test does not create a credential file.
    monkeypatch.setenv("GOVINFO_API_KEY", "secret")
    result = script._govinfo_health(settings, datetime(2026, 8, 12, tzinfo=timezone.utc))

    assert "unauthorized" in result
    assert "401" in result
    assert "secret" not in result


def test_congress_rate_scope_reports_headers_without_credentials(monkeypatch) -> None:
    script = _load_module()

    class Response:
        status_code = 200
        headers = {"X-RateLimit-Limit": "20000", "X-RateLimit-Remaining": "19999"}

    seen: list[str] = []

    def fake_get(url, *, headers, timeout):
        seen.append(headers["x-api-key"])
        assert url.endswith("/bill/119/hr/1?format=json")
        assert timeout == 10
        return Response()

    monkeypatch.setattr(script.requests, "get", fake_get)
    settings = script.Settings(
        database_url="postgresql://unused",
        env_path=Path("/tmp/no-congress-dotenv"),
    )
    monkeypatch.setenv("CONGRESSIONAL_API_KEY", "secret-a,secret-b")
    result = script._congress_rate_scope(settings, datetime(2026, 8, 13, tzinfo=timezone.utc))

    assert seen == ["secret-a", "secret-b"]
    assert "HTTP 200: 2" in result
    assert "advertised limit=20,000–20,000" in result
    assert "remaining=19,999–19,999" in result
    assert "per-key header limit observed" in result
    assert "broader aggregate limiter scope remains under test" in result
    assert "secret-a" not in result and "secret-b" not in result


def test_congress_rate_scope_defers_while_crawler_is_active(monkeypatch) -> None:
    script = _load_module()

    def unexpected_probe(*args, **kwargs):
        raise AssertionError("active crawler must not spend quota on report probes")

    monkeypatch.setattr(script.requests, "get", unexpected_probe)
    settings = script.Settings(
        database_url="postgresql://unused",
        env_path=Path("/tmp/no-congress-dotenv"),
    )
    monkeypatch.setenv("CONGRESSIONAL_API_KEY", "secret-a,secret-b")
    result = script._congress_rate_scope(
        settings,
        datetime(2026, 8, 13, tzinfo=timezone.utc),
        active_crawl=True,
    )

    assert "deferred" in result
    assert "active Congress crawler or recent quota cooldown" in result
    assert "secret-a" not in result


def test_descendant_eta_uses_audited_backlog() -> None:
    script = _load_module()
    now = datetime.now(timezone.utc)
    run = script.RunStatus(
        category="bills",
        run_id="run",
        mode="backfill_descendants",
        status="running",
        started_at=now - timedelta(hours=1),
        root_started_at=None,
        fetched=1,
        successful=1,
        failed=0,
        skipped=0,
        recent_successes=1000,
        expected=1,
        latest_congress=96,
    )

    rate, eta = script._rate_and_eta(run, child_backlog=2000)

    assert rate == "4000/h"
    assert eta == "30m"


def test_descendant_eta_uses_fixed_rolling_window_for_new_partition() -> None:
    script = _load_module()
    now = datetime.now(timezone.utc)
    run = script.RunStatus(
        category="amendments",
        run_id="run",
        mode="backfill_descendants",
        status="running",
        started_at=now - timedelta(minutes=2),
        root_started_at=None,
        fetched=4,
        successful=4,
        failed=0,
        skipped=16,
        recent_successes=300,
        expected=4,
        latest_congress=108,
    )

    rate, eta = script._rate_and_eta(run, child_backlog=1200)

    assert rate == "1200/h"
    assert eta == "1h 0m"


def test_aggregate_descendant_eta_uses_audited_backlog() -> None:
    script = _load_module()
    now = datetime.now(timezone.utc)
    run = script.RunStatus(
        category="amendments",
        run_id="run",
        mode="aggregate",
        status="running",
        started_at=now - timedelta(hours=1),
        root_started_at=None,
        fetched=104,
        successful=58,
        failed=0,
        skipped=46,
        recent_successes=300,
        expected=104,
        latest_congress=114,
    )

    rate, eta = script._rate_and_eta(run, child_backlog=1200)

    assert rate == "1200/h"
    assert eta == "1h 0m"


def test_descendant_report_distinguishes_stored_edge_traversal_from_network_idle() -> None:
    script = _load_module()
    now = datetime.now(timezone.utc)
    run = script.RunStatus(
        category="bills",
        run_id="run",
        mode="backfill_descendants",
        status="running",
        started_at=now - timedelta(hours=1),
        root_started_at=None,
        fetched=100,
        successful=0,
        failed=0,
        skipped=100,
        recent_successes=0,
        expected=100,
        latest_congress=104,
        recent_skips=2500,
    )

    rate, eta = script._rate_and_eta(run, child_backlog=2000)

    assert rate == "0/h network; 10000/h traversal"
    assert eta == "pending network edges"


def test_render_report_does_not_project_stale_rate_during_quota_cooldown() -> None:
    script = _load_module()
    now = datetime.now(timezone.utc)
    run = script.RunStatus(
        category="bills",
        run_id="run",
        mode="backfill_descendants",
        status="running",
        started_at=now - timedelta(hours=1),
        root_started_at=None,
        fetched=10,
        successful=10,
        failed=0,
        skipped=0,
        recent_successes=0,
        expected=100,
        latest_congress=98,
    )

    report = script.render_report(
        statuses=[run],
        coverage={},
        reconciliation=[],
        generated_at=now,
        availability_guard="State: **quota_cooldown** (last check now; 2-minute window: 0 failures, 0 successes, 0 HTTP 503s, 0 HTTP 429s).",
    )

    assert "| bills | 98 | backfill_descendants | waiting_quota | 10 (10/0/0) | quota cooldown | waiting for reset |" in report
    assert "1220d" not in report
    assert "Library of Congress-only coverage" not in report


def test_render_report_includes_shared_govinfo_window_frontier() -> None:
    script = _load_module()
    start = datetime(2026, 8, 24, 4, 29, 45, tzinfo=timezone.utc)
    end = datetime(2026, 8, 24, 4, 53, 9, tzinfo=timezone.utc)
    windows = [
        script.GovInfoWindowStatus(
            category=category,
            completed_through=end,
            window_start=start,
            window_end=end,
            status="completed",
            list_pages=1,
            fetch_events=1,
            failed_events=0,
        )
        for category in (
            "govinfo-bills",
            "govinfo-committee-reports",
            "govinfo-committee-prints",
            "govinfo-hearings",
        )
    ]

    report = script.render_report(
        statuses=[],
        coverage={},
        reconciliation=[],
        generated_at=end,
        govinfo_windows=windows,
    )

    assert "Shared completed frontier: **2026-08-24 04:53 UTC**" in report
    assert "| govinfo-bills | 2026-08-24 04:53 UTC |" in report


def test_render_report_includes_congress_internal_reciprocity() -> None:
    script = _load_module()
    now = datetime(2026, 8, 24, 4, 53, 9, tzinfo=timezone.utc)
    report = script.render_report(
        statuses=[],
        coverage={},
        reconciliation=[],
        internal_reciprocity=[
            script.InternalReciprocityStatus(
                relationship_type="member_bill_cosponsor",
                relationship_count=100,
                reciprocal_count=97,
                unavailable_count=3,
                contradiction_count=0,
                unclassified_count=0,
                reciprocal_rate=0.97,
                contradiction_rate=0.0,
                quality_status="review",
            )
        ],
        generated_at=now,
    )

    assert "## Congress.gov internal reciprocity" in report
    assert "| member_bill_cosponsor | 100 | 97 | 97.00% | 3 | 0 | 0 | review |" in report


def test_render_report_includes_govinfo_identity_review_buckets() -> None:
    script = _load_module()
    now = datetime(2026, 8, 24, 4, 53, 9, tzinfo=timezone.utc)
    report = script.render_report(
        statuses=[],
        coverage={},
        reconciliation=[],
        govinfo_identity_exceptions=[
            script.GovInfoIdentityExceptionStatus("ambiguous_member_name", 16),
            script.GovInfoIdentityExceptionStatus("composite_member_parent", 17),
        ],
        generated_at=now,
    )

    assert "## GovInfo member-reference identity review" in report
    assert "| ambiguous_member_name | 16 |" in report
    assert "| composite_member_parent | 17 |" in report


def test_render_report_includes_committee_print_census() -> None:
    script = _load_module()
    now = datetime(2026, 8, 27, 8, 20, tzinfo=timezone.utc)
    report = script.render_report(
        statuses=[],
        coverage={},
        reconciliation=[],
        committee_print_metrics=script.CommitteePrintMetricStatus(
            congress_print_count=1877,
            govinfo_package_count=6959,
            govinfo_representative_count=6839,
            exact_match_count=1524,
            congress_only_count=353,
            govinfo_only_representative_count=5315,
            unparseable_package_count=1,
            duplicate_variant_package_count=120,
            excluded_non_cprt_count=825,
            exact_title_agreement_count=1511,
            exact_match_rate=1524 / 1877,
            quality_status="informational_source_boundary_review",
            observed_at=now,
        ),
        generated_at=now,
    )

    assert "## Committee-print CPRT census" in report
    assert "| 1877 | 6959 | 6839 | 1524 | 353 | 5315 | 120 | 1 | 825 | 81.19% | 1511 |" in report
    assert "informational_source_boundary_review" in report


def test_render_report_preserves_progress_for_active_reserve_lane() -> None:
    script = _load_module()
    now = datetime.now(timezone.utc)
    run = script.RunStatus(
        category="amendments",
        run_id="run",
        mode="backfill_refresh_descendants",
        status="running",
        started_at=now - timedelta(hours=1),
        root_started_at=None,
        fetched=1200,
        successful=1200,
        failed=0,
        skipped=0,
        recent_successes=300,
        expected=2000,
        latest_congress=117,
    )

    report = script.render_report(
        statuses=[run],
        coverage={},
        reconciliation=[],
        generated_at=now,
        availability_guard="State: **quota_cooldown** (last check now; 2-minute window: 0 failures, 241 successes, 0 HTTP 503s, 0 HTTP 429s).",
    )

    assert "| amendments | 117 | backfill_refresh_descendants | running |" in report
    assert "| amendments | 117 | backfill_refresh_descendants | waiting_quota |" not in report
    assert "1200/h" in report


def test_render_report_preserves_stored_traversal_during_quota_cooldown() -> None:
    script = _load_module()
    now = datetime.now(timezone.utc)
    run = script.RunStatus(
        category="bills",
        run_id="run",
        mode="backfill_descendants",
        status="running",
        started_at=now - timedelta(minutes=5),
        root_started_at=None,
        fetched=100,
        successful=0,
        failed=0,
        skipped=100,
        recent_successes=0,
        recent_skips=1000,
        expected=100,
        latest_congress=95,
    )
    report = script.render_report(
        statuses=[run],
        coverage={},
        reconciliation=[],
        generated_at=now,
        availability_guard="State: **quota_cooldown** (last check now; 2-minute window: 0 failures, 0 successes, 0 HTTP 503s, 0 HTTP 429s).",
    )

    assert "waiting_quota" not in report
    assert "quota cooldown" not in report
    assert "traversal" in report


def test_render_report_does_not_regress_to_supplemental_probe_congress() -> None:
    script = _load_module()
    now = datetime.now(timezone.utc)
    run = script.RunStatus(
        category="bills",
        run_id="probe",
        mode="full",
        status="completed",
        started_at=now - timedelta(minutes=5),
        root_started_at=None,
        fetched=1,
        successful=1,
        failed=0,
        skipped=0,
        recent_successes=0,
        recent_skips=0,
        expected=1,
        latest_congress=107,
    )

    report = script.render_report(
        statuses=[run],
        coverage={"bills": (100, 100, 100, 0, 0, 119)},
        reconciliation=[],
        generated_at=now,
    )

    assert "| bills | 119 | full | completed |" in report
    assert "| bills | 107 | full | completed |" not in report


def test_eta_uses_observed_seed_total_while_descendants_are_discovered() -> None:
    script = _load_module()
    now = datetime.now(timezone.utc)
    run = script.RunStatus(
        category="hearings",
        run_id="run",
        mode="full",
        status="running",
        started_at=now - timedelta(hours=1),
        root_started_at=None,
        fetched=5364,
        successful=5000,
        failed=1,
        skipped=363,
        recent_successes=1000,
        expected=5364,
        latest_congress=119,
        observed_total=35411,
    )

    rate, eta = script._rate_and_eta(run)

    assert rate == "4000/h"
    assert eta == "7h 36m"


def test_incremental_eta_uses_changed_descendant_ledger_not_full_seed_total() -> None:
    script = _load_module()
    now = datetime.now(timezone.utc)
    run = script.RunStatus(
        category="hearings",
        run_id="run",
        mode="incremental",
        status="running",
        started_at=now - timedelta(hours=1),
        root_started_at=None,
        fetched=2700,
        successful=2700,
        failed=0,
        skipped=0,
        recent_successes=1000,
        expected=2800,
        latest_congress=119,
        observed_total=35411,
    )

    rate, eta = script._rate_and_eta(run)

    assert rate == "4000/h"
    assert eta == "2m"
