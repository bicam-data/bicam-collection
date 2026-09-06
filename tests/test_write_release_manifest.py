from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "write_release_manifest.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("write_release_manifest", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_records_preserve_column_order_and_values() -> None:
    script = _load_module()
    rows = script._records(
        [("bills", 3, Decimal("1.0"))],
        ("category", "expected", "rate"),
    )
    assert rows == [{"category": "bills", "expected": 3, "rate": Decimal("1.0")}]


def test_manifest_adds_timestamp_without_mutating_snapshot() -> None:
    script = _load_module()
    generated_at = datetime(2026, 8, 28, 2, 40, tzinfo=timezone.utc)
    snapshot = {"schema_version": 1, "scope": {"categories": ["bills"]}}
    manifest = script.build_manifest(generated_at=generated_at, snapshot=snapshot)

    assert manifest["generated_at"] is generated_at
    assert manifest["scope"]["categories"] == ["bills"]
    assert "generated_at" not in snapshot


def test_json_default_handles_db_values() -> None:
    script = _load_module()
    value = datetime(2026, 8, 28, 2, 40, tzinfo=timezone.utc)
    assert script._json_default(value) == "2026-08-28T02:40:00+00:00"
    assert script._json_default(date(2026, 8, 28)) == "2026-08-28"
    assert script._json_default(Decimal("0.975")) == "0.975"


def test_window_coverage_records_are_serializable() -> None:
    script = _load_module()
    rows = [
        (
            "govinfo-bills",
            97,
            97,
            0,
            0,
            datetime(1789, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 8, 28, 2, 40, 16, tzinfo=timezone.utc),
            0,
            True,
        )
    ]
    records = script._records(
        rows,
        (
            "category",
            "window_count",
            "completed_window_count",
            "failed_window_count",
            "running_window_count",
            "first_window_start",
            "last_window_end",
            "gap_or_overlap_count",
            "all_windows_completed",
        ),
    )
    rendered = json.dumps(records, default=script._json_default)
    assert "govinfo-bills" in rendered
    assert '"gap_or_overlap_count": 0' in rendered
