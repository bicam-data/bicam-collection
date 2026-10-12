from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


def load(name):
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


AUDIT = load("audit_bill_version_candidate")
REVIEW = load("evaluate_research_reviews")


def write_review(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=[*REVIEW.IDENTITY, "reviewer", "decision", "source_evidence", "notes"])
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def packet(tmp_path):
    directory = tmp_path / "audit"
    directory.mkdir()
    row = dict(case_id="case-1", kind="linkage", package_id="BILLS-118hr1ih", input_sha256="input", reviewer="", decision="", source_evidence="", notes="")
    write_review(directory / "review.csv", [row])
    (directory / "evidence.json").write_text("{}")
    (directory / "traces.json").write_text("[]")
    (directory / "report.json").write_text(json.dumps({"input_sha256": "input", "files": {name: hashlib.sha256((directory / name).read_bytes()).hexdigest() for name in ("review.csv", "evidence.json", "traces.json")}}))
    return directory, row, tmp_path / "reviews.csv"


def test_blank_packet_never_claims_accuracy(packet):
    directory, _, _ = packet
    report = REVIEW.evaluate(directory, directory / "review.csv")
    assert report["completed_case_count"] == 0
    assert not report["review_packet_complete"]
    assert report["population_precision"] is None
    assert report["population_recall"] is None


def test_completed_review_requires_attestation_but_not_release_ready(packet):
    directory, row, path = packet
    row.update(reviewer="independent reviewer", decision="correct_link", source_evidence="source document checked")
    write_review(path, [row])
    assert not REVIEW.evaluate(directory, path)["review_packet_complete"]
    report = REVIEW.evaluate(directory, path, independence_attested=True)
    assert report["review_packet_complete"]
    assert not report["research_release_ready"]


@pytest.mark.parametrize("field,value", [("decision", "unresolved"), ("reviewer", " "), ("source_evidence", "")])
def test_incomplete_review_remains_pending(packet, field, value):
    directory, row, path = packet
    row.update(reviewer="reviewer", decision="correct_link", source_evidence="source")
    row[field] = value
    write_review(path, [row])
    assert not REVIEW.evaluate(directory, path, independence_attested=True)["review_packet_complete"]


@pytest.mark.parametrize("field,value", [("case_id", "unknown"), ("input_sha256", "wrong"), ("decision", "pass")])
def test_invalid_review_rejected(packet, field, value):
    directory, row, path = packet
    row[field] = value
    write_review(path, [row])
    with pytest.raises(ValueError):
        REVIEW.evaluate(directory, path)


def test_duplicate_review_rejected(packet):
    directory, row, path = packet
    write_review(path, [row, row])
    with pytest.raises(ValueError, match="duplicate"):
        REVIEW.evaluate(directory, path)


def test_modified_input_rejected(packet):
    directory, _, _ = packet
    (directory / "evidence.json").write_text("changed")
    with pytest.raises(ValueError, match="artifact differs"):
        REVIEW.evaluate(directory, directory / "review.csv")


def test_comparison_preserves_added_removed_changed():
    report = AUDIT.changes([{"id": "a", "v": 1}, {"id": "c", "v": 3}], [{"id": "a", "v": 2}, {"id": "b", "v": 4}], "id")
    assert report["added"] == ["b"] and report["removed"] == ["c"]
    assert report["changed"] == [{"key": "a", "fields": {"v": {"before": 1, "after": 2}}}]
    with pytest.raises(ValueError):
        AUDIT.changes([{"id": "a"}, {"id": "a"}], [], "id")


def test_invalid_sample_rejected_before_queries(tmp_path):
    with pytest.raises(ValueError):
        AUDIT.audit(object(), ["BILLS-118hr1ih"] * 9, tmp_path / "must-not-exist")
    assert not (tmp_path / "must-not-exist").exists()
