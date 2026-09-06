from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import importlib.util
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts" / "backfill_congress_summaries.py"
MODULE_SPEC = importlib.util.spec_from_file_location("backfill_congress_summaries", SCRIPT)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
MODULE = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = MODULE
MODULE_SPEC.loader.exec_module(MODULE)


def test_windows_are_bounded_and_overlap_without_gaps() -> None:
    start = datetime(2021, 1, 1, tzinfo=timezone.utc)
    end = datetime(2021, 3, 1, tzinfo=timezone.utc)
    result = MODULE.windows(start, end, window_days=31, overlap_hours=24)
    assert len(result) == 2
    assert result[0].start == start
    assert result[-1].end == end
    assert result[1].start == result[0].end - MODULE.timedelta(hours=24)
    assert result[1].start < result[0].end


def test_checkpoint_round_trip_is_category_specific(tmp_path: Path) -> None:
    checkpoint = tmp_path / "summaries.json"
    completed = {"2021-01-01T00:00:00Z/2021-02-01T00:00:00Z"}
    MODULE.save_checkpoint(checkpoint, completed)
    assert MODULE.load_checkpoint(checkpoint) == completed


def test_dry_run_limits_pending_windows(tmp_path: Path) -> None:
    args = MODULE.parse_args(
        [
            "--start",
            "2021-01-01T00:00:00Z",
            "--until",
            "2021-04-01T00:00:00Z",
            "--max-windows",
            "1",
            "--checkpoint",
            str(tmp_path / "checkpoint.json"),
            "--dry-run",
        ]
    )
    assert args.max_windows == 1
