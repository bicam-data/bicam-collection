from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location("aggregate_reviews", SCRIPTS / "aggregate_research_reviews.py")
MODULE = importlib.util.module_from_spec(spec)
spec.loader.exec_module(MODULE)


@pytest.fixture
def packet(tmp_path):
    audit = tmp_path / "audit"
    audit.mkdir()
    occurrence = "occurrence-1"
    case_id = hashlib.sha256(json.dumps([occurrence, "text"]).encode()).hexdigest()
    row = dict(case_id=case_id, kind="text", package_id="", input_sha256="input", reviewer="", decision="", source_evidence="", notes="")
    with (audit / "review.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    (audit / "evidence.json").write_text(json.dumps({"cases": [{"occurrence_id": occurrence}]}))
    (audit / "traces.json").write_text("[]")
    (audit / "report.json").write_text(json.dumps({"input_sha256": "input", "files": {
        name: hashlib.sha256((audit / name).read_bytes()).hexdigest()
        for name in ("review.csv", "evidence.json", "traces.json")}}))
    paths = []
    for i in range(3):
        path = tmp_path / f"reviewer-{i}.json"
        path.write_text(json.dumps({"reviewer": f"reviewer-{i}", "reviews": [{
            "case_id": case_id, "occurrence_id": occurrence, "kind": "text", "decision": "no_extracted_text",
            "source_document_inspected": False, "source_evidence": "frozen:raw_text=null", "notes": "No prose."}]}))
        paths.append(path)
    return audit, paths


def change(path, **fields):
    value = json.loads(path.read_text())
    value["reviews"][0].update(fields)
    path.write_text(json.dumps(value))


def test_consensus_never_claims_human_validation(packet):
    rows, report = MODULE.aggregate(*packet)
    assert rows[0]["decision"] == "no_extracted_text"
    assert report["agreement_counts"] == {"unanimous_resolved": 1}
    assert not report["independence_attested"]
    assert not report["research_release_ready"]
    assert len(report["cases"][0]["votes"]) == 3


def test_majority_is_not_automatic_adjudication(packet):
    audit, paths = packet
    change(paths[2], decision="unresolved")
    rows, report = MODULE.aggregate(audit, paths)
    assert rows[0]["decision"] == "unresolved"
    assert report["agreement_counts"] == {"disagreement": 1}


def test_explicit_adjudication_preserves_votes(packet, tmp_path):
    audit, paths = packet
    change(paths[2], decision="unresolved")
    path = tmp_path / "adjudication.json"
    path.write_text(json.dumps({"reviews": json.loads(paths[0].read_text())["reviews"]}))
    rows, report = MODULE.aggregate(audit, paths, path)
    assert rows[0]["decision"] == "no_extracted_text"
    assert report["adjudicated_count"] == 1
    assert report["cases"][0]["vote_counts"] == {"no_extracted_text": 2, "unresolved": 1}


@pytest.mark.parametrize("mutation", [
    {"source_evidence": ""}, {"source_document_inspected": "true"},
    {"decision": "faithful", "source_document_inspected": False},
    {"case_id": "wrong"}, {"occurrence_id": "wrong"}, {"kind": "linkage"},
])
def test_invalid_votes_rejected(packet, mutation):
    audit, paths = packet
    change(paths[0], **mutation)
    with pytest.raises(ValueError):
        MODULE.aggregate(audit, paths)


def test_duplicate_reviewer_rejected(packet):
    audit, paths = packet
    with pytest.raises(ValueError, match="duplicate reviewer"):
        MODULE.aggregate(audit, [paths[0], paths[0]])


def test_tampered_original_rejected(packet):
    audit, paths = packet
    (audit / "traces.json").write_text("changed")
    with pytest.raises(ValueError, match="artifact differs"):
        MODULE.aggregate(audit, paths)
