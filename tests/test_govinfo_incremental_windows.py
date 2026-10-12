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
