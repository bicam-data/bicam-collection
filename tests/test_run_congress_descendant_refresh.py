from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_congress_descendant_refresh.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("run_congress_descendant_refresh", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_checkpoint_cursor_advances_atomically_and_resumes(tmp_path: Path) -> None:
    script = _load_module()
    checkpoint = tmp_path / "descendant-cursor.json"
    payload = script._load_checkpoint(checkpoint)
    cursor = script._category_cursor(payload, category="bills", root_count=1200)
    assert cursor == {
        "after_endpoint_url": None,
        "cycle_start_root_count": 1200,
        "observed_root_count": 1200,
        "completed": False,
    }
    cursor["after_endpoint_url"] = "https://api.congress.gov/v3/bill/118/hr/500"
    script._save_checkpoint(checkpoint, payload)

    resumed = script._load_checkpoint(checkpoint)
    resumed_cursor = script._category_cursor(resumed, category="bills", root_count=1200)
    assert resumed_cursor["after_endpoint_url"] == "https://api.congress.gov/v3/bill/118/hr/500"
    assert not resumed_cursor["completed"]


def test_completed_cursor_only_reopens_when_new_roots_arrive(tmp_path: Path) -> None:
    script = _load_module()
    checkpoint = tmp_path / "descendant-cursor.json"
    payload = {
        "version": 2,
        "categories": {
            "bills": {
                "after_endpoint_url": "https://api.congress.gov/v3/bill/119/s/9999",
                "cycle_start_root_count": 1200,
                "observed_root_count": 1200,
                "completed": True,
            }
        },
    }
    script._save_checkpoint(checkpoint, payload)
    resumed = script._load_checkpoint(checkpoint)
    assert script._category_cursor(resumed, category="bills", root_count=1200)["completed"]
    reopened = script._category_cursor(resumed, category="bills", root_count=1500)
    assert reopened["after_endpoint_url"] is None
    assert reopened["cycle_start_root_count"] == 1500
    assert not reopened["completed"]


def test_completed_cursor_reopens_when_spec_changes() -> None:
    script = _load_module()
    payload = {
        "version": 2,
        "categories": {
            "nominations": {
                "after_endpoint_url": "https://api.congress.gov/v3/nomination/119/999",
                "cycle_start_root_count": 100,
                "observed_root_count": 100,
                "completed": True,
                "spec_fingerprint": "old",
            }
        },
    }

    cursor = script._category_cursor(
        payload,
        category="nominations",
        root_count=100,
        spec_fingerprint="new",
    )

    assert cursor["after_endpoint_url"] is None
    assert not cursor["completed"]
    assert cursor["spec_fingerprint"] == "new"


def test_version_one_offset_checkpoint_is_reset_for_safe_replay(tmp_path: Path) -> None:
    script = _load_module()
    checkpoint = tmp_path / "descendant-cursor.json"
    script._save_checkpoint(
        checkpoint,
        {
            "version": 1,
            "categories": {
                "amendments": {
                    "next_offset": 20500,
                    "root_count": 128516,
                    "completed": False,
                }
            },
        },
    )

    migrated = script._load_checkpoint(checkpoint)
    assert migrated["version"] == 2
    raw_cursor = migrated["categories"]["amendments"]
    assert raw_cursor["after_endpoint_url"] is None
    assert not raw_cursor["completed"]
    assert raw_cursor["reset_from_version"] == 1
