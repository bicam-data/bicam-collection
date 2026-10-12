from __future__ import annotations

import csv
import gzip
import hashlib
import importlib.util
import json
import io
import sys
from pathlib import Path

import pytest


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


@pytest.mark.parametrize("marker, record", [
    ("", ',"",hello,"line one\r\nline two, with ""quotes"""\r\n'),
    (r"\N", '\\N,"",hello,"line one\r\nline two, with ""quotes"""\r\n'),
])
def test_postgres_csv_preserves_null_empty_and_multiline(marker, record) -> None:
    assert list(MODULE._csv_rows(io.StringIO(record, newline=""), marker)) == [
        [None, "", "hello", 'line one\r\nline two, with "quotes"']
    ]


def test_postgres_csv_preserves_literal_null_marker() -> None:
    assert list(MODULE._csv_rows(io.StringIO('\\N,"\\N",\n'), r"\N")) == [[None, r"\N", ""]]


def _replace_artifact(manifest: Path, content: str) -> None:
    document = json.loads(manifest.read_text())
    artifact = manifest.parent / document["tables"][0]["file"]
    with gzip.open(artifact, "wt", encoding="utf-8", newline="") as stream:
        stream.write(content)
    entry = document["tables"][0]
    entry["bytes"] = artifact.stat().st_size
    entry["sha256"] = hashlib.sha256(artifact.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(document))


def test_formatted_text_links_do_not_count_as_extracted_content(tmp_path: Path) -> None:
    manifest = _candidate(tmp_path, rows=[
        ["hr1-119", "119", "House", r"\N", "https://example.test/one.htm"],
        ["hr2-119", "119", "House", "  \n ", "https://example.test/two.htm"],
        ["hr3-119", "119", "House", "Bill text", "https://example.test/three.htm"],
    ])
    document = json.loads(manifest.read_text())
    document["tables"][0]["columns"][-1]["name"] = "formatted_text"
    manifest.write_text(json.dumps(document))
    with gzip.open(manifest.parent / "bills_texts.csv.gz", "rt", newline="") as stream:
        content = stream.read().replace("raw_text,pdf\n", "raw_text,formatted_text\n", 1)
    _replace_artifact(manifest, content)
    text = MODULE.evaluate_manifest(manifest)["tables"]["bills_texts"]["text"]
    assert text["rows_with_text"] == 1
    assert text["rows_without_text"] == 2
    assert text["rows_with_links"] == 3
    assert text["rows_with_text_and_links"] == 1


def test_legacy_default_null_is_distinct_from_empty_text(tmp_path: Path) -> None:
    manifest = _candidate(tmp_path, rows=[["1"]] * 2)
    document = json.loads(manifest.read_text())
    del document["serialization"]
    manifest.write_text(json.dumps(document))
    _replace_artifact(manifest, 'bill_id,congress,chamber,raw_text,pdf\na,119,House,,\nb,119,House,"",\n')
    report = MODULE.evaluate_manifest(manifest)
    assert report["tables"]["bills_texts"]["null_counts"]["raw_text"] == 1
    assert report["tables"]["bills_texts"]["blank_counts"]["raw_text"] == 1
    assert report["serialization_basis"] == "legacy_postgres_copy_csv_default"


def test_bound_does_not_trust_underreported_manifest_count(tmp_path: Path) -> None:
    manifest = _candidate(tmp_path, rows=[["a", "119", "House", "x", ""]] * 2)
    document = json.loads(manifest.read_text())
    document["tables"][0]["row_count"] = 1
    manifest.write_text(json.dumps(document))
    assert MODULE.evaluate_manifest(manifest, max_rows_per_table=1)["complete_inventory_scan"] is False
    with pytest.raises(ValueError, match="row count"):
        MODULE.evaluate_manifest(manifest)


def test_duplicate_inventory_is_rejected(tmp_path: Path) -> None:
    manifest = _candidate(tmp_path, rows=[])
    document = json.loads(manifest.read_text())
    document["tables"].append(document["tables"][0])
    manifest.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="duplicate candidate table"):
        MODULE.evaluate_manifest(manifest)


def test_output_cannot_mutate_candidate_and_default_is_external(tmp_path: Path) -> None:
    manifest = _candidate(tmp_path, rows=[])
    before = {p.name: p.read_bytes() for p in manifest.parent.iterdir()}
    with pytest.raises(ValueError, match="outside the immutable candidate"):
        MODULE.main(["--manifest", str(manifest), "--output", str(manifest.parent / "report.json")])
    assert MODULE.main(["--manifest", str(manifest)]) == 0
    assert (tmp_path / "quality-reports" / f"{manifest.parent.name}.json").is_file()
    assert before == {p.name: p.read_bytes() for p in manifest.parent.iterdir()}
    with pytest.raises(FileExistsError):
        MODULE.main(["--manifest", str(manifest)])


def test_tampered_payload_and_unknown_serialization_are_rejected(tmp_path: Path) -> None:
    manifest = _candidate(tmp_path, rows=[])
    document = json.loads(manifest.read_text())
    document["serialization"]["encoding"] = "UTF-16"
    manifest.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="unsupported candidate CSV serialization"):
        MODULE.evaluate_manifest(manifest)
    document["serialization"]["encoding"] = "UTF-8"
    manifest.write_text(json.dumps(document))
    with (manifest.parent / "bills_texts.csv.gz").open("ab") as stream:
        stream.write(b"tampered")
    with pytest.raises(ValueError, match="artifact bytes differ"):
        MODULE.evaluate_manifest(manifest)


def test_nonpositive_bound_is_rejected_by_library(tmp_path: Path) -> None:
    manifest = _candidate(tmp_path, rows=[])
    with pytest.raises(ValueError, match="positive"):
        MODULE.evaluate_manifest(manifest, max_rows_per_table=0)
