from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.alert_lane_failure import build_alert_line
from scripts.record_lane_run import (
    build_lane_run,
    parse_show_output,
    parse_systemd_timestamp,
)
from scripts.report_lane_status import (
    LaneRunRow,
    UnitStatus,
    parse_timer_schedule,
    render_json,
    render_markdown,
    summarize_units,
)

UTC = timezone.utc
RECORDED_AT = datetime(2026, 9, 6, 21, 10, 0, tzinfo=UTC)


def test_parse_systemd_timestamp_reads_utc_show_output() -> None:
    parsed = parse_systemd_timestamp("Sun 2026-09-06 21:07:57 UTC")
    assert parsed == datetime(2026, 9, 6, 21, 7, 57, tzinfo=UTC)


def test_parse_systemd_timestamp_treats_empty_as_unset() -> None:
    assert parse_systemd_timestamp("") is None


def test_parse_show_output_keeps_empty_values() -> None:
    properties = parse_show_output(
        "ExecMainStartTimestamp=Sun 2026-09-06 21:07:57 UTC\n"
        "ExecMainExitTimestamp=\n"
        "InactiveEnterTimestamp=Sun 2026-09-06 20:57:57 UTC\n"
    )
    assert properties == {
        "ExecMainStartTimestamp": "Sun 2026-09-06 21:07:57 UTC",
        "ExecMainExitTimestamp": "",
        "InactiveEnterTimestamp": "Sun 2026-09-06 20:57:57 UTC",
    }


def test_build_lane_run_uses_exec_timestamps_and_undashed_invocation_id() -> None:
    run = build_lane_run(
        unit="bicam-html-text.service",
        invocation_id="f4c64c5167854f2c9c6b7c10bb91191f",
        service_result="exit-code",
        exit_code="exited",
        exit_status="1",
        properties={
            "ExecMainStartTimestamp": "Sun 2026-09-06 21:07:57 UTC",
            "ExecMainExitTimestamp": "Sun 2026-09-06 21:09:30 UTC",
            # Still the previous run while ExecStopPost= is running.
            "InactiveEnterTimestamp": "Sun 2026-09-06 20:57:57 UTC",
        },
        recorded_at=RECORDED_AT,
    )
    assert run.unit == "bicam-html-text.service"
    assert run.invocation_id == uuid.UUID("f4c64c51-6785-4f2c-9c6b-7c10bb91191f")
    assert run.started_at == datetime(2026, 9, 6, 21, 7, 57, tzinfo=UTC)
    assert run.finished_at == datetime(2026, 9, 6, 21, 9, 30, tzinfo=UTC)
    assert run.service_result == "exit-code"
    assert run.exit_code == "exited"
    assert run.exit_status == "1"
    assert run.details == {
        "recorded_at": "2026-09-06T21:10:00+00:00",
        "exec_main_start_timestamp": "Sun 2026-09-06 21:07:57 UTC",
        "exec_main_exit_timestamp": "Sun 2026-09-06 21:09:30 UTC",
        "inactive_enter_timestamp": "Sun 2026-09-06 20:57:57 UTC",
    }


def test_build_lane_run_falls_back_to_recorded_at_when_exit_timestamp_missing() -> None:
    run = build_lane_run(
        unit="bicam-html-text.service",
        invocation_id="f4c64c5167854f2c9c6b7c10bb91191f",
        service_result="exec-failed",
        exit_code="exited",
        exit_status="203",
        properties={
            "ExecMainStartTimestamp": "",
            "ExecMainExitTimestamp": "",
            "InactiveEnterTimestamp": "",
        },
        recorded_at=RECORDED_AT,
    )
    assert run.started_at is None
    assert run.finished_at == RECORDED_AT


def test_alert_line_is_single_json_line_with_journal_tail() -> None:
    line = build_alert_line(
        "bicam-html-text.service",
        RECORDED_AT,
        ["Starting bicam-html-text.service...", "Traceback (most recent call last):"],
    )
    assert line.endswith("\n")
    assert line.count("\n") == 1
    assert json.loads(line) == {
        "unit": "bicam-html-text.service",
        "alerted_at": "2026-09-06T21:10:00+00:00",
        "journal": [
            "Starting bicam-html-text.service...",
            "Traceback (most recent call last):",
        ],
    }


def test_parse_timer_schedule_keeps_bicam_services_only() -> None:
    timers = json.dumps(
        [
            {
                "next": 1788729500449705,
                "left": 1788729500449705,
                "last": 1788729380432327,
                "passed": 1281004203708,
                "unit": "bicam-ingestion-report.timer",
                "activates": "bicam-ingestion-report.service",
            },
            {
                "next": None,
                "left": None,
                "last": 1788728877836204,
                "passed": 1280501607585,
                "unit": "bicam-html-text.timer",
                "activates": "bicam-html-text.service",
            },
            {
                "next": 1788744759203645,
                "left": 1788744759203645,
                "last": 1788658359203645,
                "passed": 1209982975025,
                "unit": "launchpadlib-cache-clean.timer",
                "activates": "launchpadlib-cache-clean.service",
            },
        ]
    )
    schedule = parse_timer_schedule(timers)
    assert schedule == {
        "bicam-ingestion-report.service": datetime.fromtimestamp(
            1788729500.449705, tz=UTC
        ),
        "bicam-html-text.service": None,
    }


def _row(unit: str, finished_at: datetime, result: str, minutes: float = 2) -> LaneRunRow:
    return LaneRunRow(
        unit=unit,
        started_at=finished_at - timedelta(minutes=minutes),
        finished_at=finished_at,
        service_result=result,
        exit_code="exited",
        exit_status="0" if result == "success" else "1",
    )


def test_summarize_units_counts_consecutive_failures_from_newest() -> None:
    base = datetime(2026, 9, 6, 20, 0, tzinfo=UTC)
    rows = [
        _row("bicam-html-text.service", base + timedelta(minutes=30), "exit-code"),
        _row("bicam-html-text.service", base + timedelta(minutes=20), "timeout"),
        _row("bicam-html-text.service", base + timedelta(minutes=10), "success"),
        _row("bicam-html-text.service", base, "exit-code"),
        _row("bicam-ingestion-report.service", base, "success", minutes=90),
    ]
    schedule = {
        "bicam-html-text.service": base + timedelta(minutes=40),
        "bicam-reconciliation-probe.service": None,
    }
    statuses = summarize_units(rows, schedule)
    assert [status.unit for status in statuses] == [
        "bicam-html-text.service",
        "bicam-ingestion-report.service",
        "bicam-reconciliation-probe.service",
    ]
    html_text, report, probe = statuses
    assert html_text == UnitStatus(
        unit="bicam-html-text.service",
        last_result="exit-code",
        last_exit="exited/1",
        last_finished_at="2026-09-06T20:30:00+00:00",
        last_duration_seconds=120.0,
        consecutive_failures=2,
        next_fire_at="2026-09-06T20:40:00+00:00",
    )
    assert report.consecutive_failures == 0
    assert report.last_duration_seconds == 5400.0
    assert report.next_fire_at is None
    assert probe.last_result is None
    assert probe.consecutive_failures == 0


def test_summarize_units_handles_missing_started_at() -> None:
    rows = [
        LaneRunRow(
            unit="bicam-html-text.service",
            started_at=None,
            finished_at=RECORDED_AT,
            service_result="exec-failed",
            exit_code="exited",
            exit_status="203",
        )
    ]
    (status,) = summarize_units(rows, {})
    assert status.last_duration_seconds is None
    assert status.consecutive_failures == 1


def test_render_markdown_and_json_from_fixture_rows() -> None:
    statuses = [
        UnitStatus(
            unit="bicam-html-text.service",
            last_result="exit-code",
            last_exit="exited/1",
            last_finished_at="2026-09-06T20:30:00+00:00",
            last_duration_seconds=120.0,
            consecutive_failures=2,
            next_fire_at="2026-09-06T20:40:00+00:00",
        ),
        UnitStatus(
            unit="bicam-reconciliation-probe.service",
            last_result=None,
            last_exit=None,
            last_finished_at=None,
            last_duration_seconds=None,
            consecutive_failures=0,
            next_fire_at=None,
        ),
    ]
    markdown = render_markdown(statuses, RECORDED_AT)
    assert "# BICAM lane status" in markdown
    assert "Generated: 2026-09-06T21:10:00+00:00" in markdown
    assert (
        "| bicam-html-text.service | exit-code | exited/1 | 2026-09-06T20:30:00+00:00 "
        "| 2m00s | 2 | 2026-09-06T20:40:00+00:00 |"
    ) in markdown
    assert "| bicam-reconciliation-probe.service | never ran | - | - | - | 0 | - |" in markdown

    payload = json.loads(render_json(statuses, RECORDED_AT))
    assert payload["generated_at"] == "2026-09-06T21:10:00+00:00"
    assert payload["units"][0]["unit"] == "bicam-html-text.service"
    assert payload["units"][0]["consecutive_failures"] == 2
    assert payload["units"][1]["last_result"] is None
