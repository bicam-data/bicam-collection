from __future__ import annotations

import gzip
import importlib.util
import json
from pathlib import Path
import sys

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "compare_release_candidates.py"
SPEC = importlib.util.spec_from_file_location("compare_release_candidates", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _candidate(root: Path, name: str, rows: list[dict[str, str]], *, key: list[str] | None = None) -> Path:
    directory = root / name
    directory.mkdir(parents=True)
    columns = list(rows[0]) if rows else ["bill_id", "congress", "title"]
    artifact = directory / "bills.csv.gz"
    with gzip.open(artifact, "wt", encoding="utf-8", newline="") as stream:
        stream.write(",".join(columns) + "\n")
        for row in rows:
            stream.write(",".join(row.get(column, "") for column in columns) + "\n")
    payload = artifact.read_bytes()
    manifest = {
        "schema_version": 2,
        "contract_version": 3,
        "snapshot_directory": name,
        "tables": [
            {
                "name": "bills",
                "relation": "bills",
                "file": "bills.csv.gz",
                "format": "csv.gz",
                "row_count": len(rows),
                "bytes": len(payload),
                "sha256": MODULE.hashlib.sha256(payload).hexdigest(),
                "columns": [{"name": column, "data_type": "text"} for column in columns],
                "contract_columns": {column: "text" for column in columns},
                "primary_key": key if key is not None else ["bill_id"],
            }
        ],
    }
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


def test_candidate_comparison_reports_add_remove_change_duplicate_and_congress_delta(tmp_path: Path) -> None:
    before = _candidate(
        tmp_path,
        "before",
        [
            {"bill_id": "b1", "congress": "118", "title": "old"},
            {"bill_id": "b2", "congress": "118", "title": "same"},
            {"bill_id": "b2", "congress": "118", "title": "same"},
        ],
    )
    after = _candidate(
        tmp_path,
        "after",
        [
            {"bill_id": "b1", "congress": "118", "title": "new"},
            {"bill_id": "b3", "congress": "119", "title": "added"},
        ],
    )

    report = MODULE.compare_candidates(before, after, max_examples=3)
    table = report["tables"][0]

    assert report["inventory"]["exact"] is True
    assert table["diff"]["added_rows"] == 2
    assert table["diff"]["removed_rows"] == 3
    assert table["diff"]["retained_rows"] == 0
    assert table["diff"]["changed_keys"] == 1
    assert table["duplicate_keys"]["before"]["count"] == 1
    assert table["duplicate_keys"]["after"]["count"] == 0
    assert table["by_congress"]["118"]["row_delta"] == -2
    assert table["by_congress"]["119"]["row_delta"] == 1
    assert table["before"]["hash_matches_manifest"] is True
    assert table["after"]["row_count_matches_manifest"] is True


def test_key_contract_migration_falls_back_to_complete_row_identity(tmp_path: Path) -> None:
    before = _candidate(
        tmp_path,
        "before",
        [{"bill_id": "b1", "congress": "118", "title": "one"}],
        key=["bill_id"],
    )
    after = _candidate(
        tmp_path,
        "after",
        [{"bill_id": "b1", "congress": "118", "title": "one"}],
        key=["bill_id", "congress"],
    )

    table = MODULE.compare_candidates(before, after)["tables"][0]

    assert table["key_contract"]["same"] is False
    assert table["diff"]["added_rows"] == 0
    assert table["diff"]["removed_rows"] == 0
    assert table["diff"]["retained_rows"] == 1


def test_complete_source_removal_is_reported_as_a_removed_key(tmp_path: Path) -> None:
    before = _candidate(
        tmp_path,
        "before",
        [
            {"bill_id": "b1", "congress": "118", "title": "kept"},
            {"bill_id": "b2", "congress": "118", "title": "retired"},
        ],
    )
    after = _candidate(
        tmp_path,
        "after",
        [{"bill_id": "b1", "congress": "118", "title": "kept"}],
    )

    diff = MODULE.compare_candidates(before, after)["tables"][0]["diff"]

    assert diff["removed_rows"] == 1
    assert diff["added_rows"] == 0
    assert diff["retained_rows"] == 1
    assert diff["examples"]["removed"][0]["key"] == ["b2"]


def test_candidate_manifest_and_artifacts_must_not_be_symlinked(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path, "before", [{"bill_id": "b1", "congress": "118", "title": "one"}])
    after = _candidate(tmp_path, "after", [{"bill_id": "b1", "congress": "118", "title": "one"}])
    candidate_dir = candidate.parent
    artifact = candidate_dir / "bills.csv.gz"
    artifact.unlink()
    artifact.symlink_to(after.parent / "bills.csv.gz")

    with pytest.raises(ValueError, match="missing or symlinked"):
        MODULE.compare_candidates(candidate, after)


def test_comparison_writes_machine_report_and_markdown(tmp_path: Path) -> None:
    before = _candidate(tmp_path, "before", [{"bill_id": "b1", "congress": "118", "title": "one"}])
    after = _candidate(tmp_path, "after", [{"bill_id": "b1", "congress": "118", "title": "two"}])
    report = MODULE.compare_candidates(before, after)

    rendered = MODULE.render_markdown(report)
    assert "# BICAM release candidate comparison" in rendered
    assert "`bills`" in rendered
