from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "check_release_freshness.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("check_release_freshness", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _manifest(path: Path, generated_at: datetime) -> None:
    path.write_text(
        json.dumps(
            {
                "generated_at": generated_at.isoformat(),
                "tables": [],
            }
        ),
        encoding="utf-8",
    )


def test_parse_args_defaults_to_latest_manifest() -> None:
    script = _load_module()
    args = script.parse_args([])
    assert args.manifest == Path("../reports/exports/latest/manifest.json")
    assert args.max_age_hours == 24.0
    assert args.max_stale_run_hours == 3.0
    assert args.no_file_verification is False


def test_manifest_check_rejects_stale_or_empty_snapshot(tmp_path: Path) -> None:
    script = _load_module()
    now = datetime(2026, 8, 28, 6, 0, tzinfo=timezone.utc)
    manifest_path = tmp_path / "manifest.json"
    _manifest(manifest_path, now - timedelta(hours=2))
    with pytest.raises(ValueError, match="no exported tables"):
        script._verify_manifest(manifest_path, now=now, max_age_hours=24, verify_files=False)

    manifest_path.write_text(
        json.dumps(
            {
                "generated_at": (now - timedelta(hours=25)).isoformat(),
                "tables": [{"file": "bills.csv.gz", "sha256": "x", "row_count": 0}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="hours old"):
        script._verify_manifest(manifest_path, now=now, max_age_hours=24, verify_files=False)


def test_manifest_file_verification_handles_quoted_multiline_csv(tmp_path: Path) -> None:
    script = _load_module()
    import gzip

    export_path = tmp_path / "bills.csv.gz"
    with gzip.open(export_path, "wt", newline="") as stream:
        stream.write('id,title\n1,"title with\nnewline"\n')
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-08-28T06:00:00+00:00",
                "tables": [
                    {
                        "file": "bills.csv.gz",
                        "sha256": __import__("hashlib").sha256(export_path.read_bytes()).hexdigest(),
                        "row_count": 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    result = script._verify_manifest(
        manifest_path,
        now=datetime(2026, 8, 28, 6, 1, tzinfo=timezone.utc),
        max_age_hours=24,
        verify_files=True,
    )
    assert result["verified_files"] == 1


def test_manifest_check_rejects_path_traversal(tmp_path: Path) -> None:
    script = _load_module()
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-08-28T06:00:00+00:00",
                "tables": [{"file": "../secret", "sha256": "x", "row_count": 0}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unsafe"):
        script._verify_manifest(
            manifest_path,
            now=datetime(2026, 8, 28, 6, 1, tzinfo=timezone.utc),
            max_age_hours=24,
            verify_files=True,
        )


class _FakeResult:
    def __init__(self, row: tuple[int, ...]) -> None:
        self.row = row

    def fetchone(self) -> tuple[int, ...]:
        return self.row


class _FakeConnection:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.rows = iter(
            (
                (0, 13),  # root detail coverage
                (0,),  # child coverage
                (0, 0, 0),  # cross-source reconciliation
                (11, 0, 0, 0),  # reciprocity acceptance
                (0,),  # GovInfo identity-quality issues
                (0,),  # stale runs
            )
        )

    def execute(self, query: str, params: object = None) -> _FakeResult:
        del params
        self.calls.append(query)
        return _FakeResult(next(self.rows))


def test_database_checks_include_reciprocity_and_govinfo_quality() -> None:
    script = _load_module()
    connection = _FakeConnection()

    result = script._database_checks(connection, max_stale_run_hours=3)

    assert result["reciprocity_metric_family_count"] == 11
    assert result["reciprocity_release_rejected_count"] == 0
    assert result["reciprocity_contradiction_count"] == 0
    assert result["reciprocity_unclassified_count"] == 0
    assert result["govinfo_identity_quality_issue_count"] == 0
    assert len(connection.calls) == 6
