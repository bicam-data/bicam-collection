import importlib.util
from datetime import datetime
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyarrow")
scripts = Path(__file__).parents[1] / "scripts"
capture_spec = importlib.util.spec_from_file_location(
    "capture_timestamp_field_evidence", scripts / "capture_timestamp_field_evidence.py")
capture = importlib.util.module_from_spec(capture_spec)
sys.path.insert(0, str(scripts))
try:
    capture_spec.loader.exec_module(capture)
finally:
    sys.path.pop(0)


def observation(raw_id, first_seen, update):
    return {"raw_id": raw_id, "first_seen_at": datetime.fromisoformat(first_seen),
            "response": {"congress": {"number": 119, "updateDate": update}}}


def test_choose_latest_keeps_one_pre_snapshot_version_per_raw_endpoint():
    selected = capture.choose_latest([
        observation("one", "2025-01-01T00:00:00+00:00", "2025-01-01T00:00:00Z"),
        observation("one", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00Z"),
        observation("two", "2025-02-01T00:00:00+00:00", "2025-02-01T00:00:00Z"),
    ])
    assert {(row["raw_id"], row["response"]["congress"]["updateDate"]) for row in selected} == {
        ("one", "2026-01-01T00:00:00Z"), ("two", "2025-02-01T00:00:00Z")}


def test_source_history_returns_system_code_and_history_without_guessing():
    code, history = capture.source_history({"response": {"committee": {
        "systemCode": "hsxx00", "history": [{"startDate": "2025-01-03T00:00:00Z",
                                                  "updateDate": "2026-01-01T00:00:00Z"}]}}})
    assert code == "hsxx00" and history[0]["startDate"].startswith("2025-01-03")
