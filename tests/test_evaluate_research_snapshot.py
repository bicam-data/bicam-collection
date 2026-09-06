from __future__ import annotations

import csv
import gzip
import hashlib
import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "evaluate_research_snapshot.py"
SPEC = importlib.util.spec_from_file_location("evaluate_research_snapshot", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _candidate(tmp_path: Path, *, rows: list[list[str]]) -> Path:
    candidate = tmp_path / "20260906T000000Z"
    candidate.mkdir()
    artifact = candidate / "bills_texts.csv.gz"
    with gzip.open(artifact, "wt", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["bill_id", "congress", "chamber", "raw_text", "pdf"])
        writer.writerows(rows)
    payload = artifact.read_bytes()
    manifest = {
        "schema_version": 2,
        "generated_at": "2026-09-06T00:00:00+00:00",
        "database_snapshot_at": "2026-09-06T00:00:00+00:00",
        "serialization": {
            "format": "csv.gz",
            "encoding": "UTF-8",
            "newline": "LF",
            "header": True,
            "csv_null": r"\N",
        },
        "tables": [
            {
                "name": "bills_texts",
                "file": "bills_texts.csv.gz",
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "row_count": len(rows),
                "columns": [{"name": name} for name in ["bill_id", "congress", "chamber", "raw_text", "pdf"]],
            }
        ],
    }
    path = candidate / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def test_evaluator_distinguishes_null_blank_and_text_links(tmp_path: Path) -> None:
    manifest = _candidate(
        tmp_path,
        rows=[
            ["hr1-119", "119", "House", "text", r"\N"],
            ["hr2-119", "119", "House", "", "https://example.test/2.pdf"],
            ["s3-118", "118", "Senate", r"\N", "https://example.test/3.pdf"],
        ],
    )

    report = MODULE.evaluate_manifest(manifest)
    table = report["tables"]["bills_texts"]

    assert report["null_semantics"] == "distinguishable"
    assert table["row_count"] == 3
    assert table["null_counts"]["raw_text"] == 1
    assert table["blank_counts"]["raw_text"] == 1
    assert table["dimensions"]["congress"] == {"118": 1, "119": 2}
    assert table["text"]["rows_with_text"] == 1
    assert table["text"]["rows_with_links"] == 2
    assert table["text"]["rows_with_text_and_links"] == 0


def test_bounded_scan_is_explicitly_partial(tmp_path: Path) -> None:
    manifest = _candidate(
        tmp_path,
        rows=[["hr1-119", "119", "House", "text", r"\N"], ["hr2-119", "119", "House", "text", r"\N"]],
    )

    report = MODULE.evaluate_manifest(manifest, max_rows_per_table=1)

    table = report["tables"]["bills_texts"]
    assert table["row_count"] == 1
    assert table["manifest_row_count"] == 2
    assert table["scan_complete"] is False
