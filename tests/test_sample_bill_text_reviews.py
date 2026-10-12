from __future__ import annotations

import csv
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


PATH = Path(__file__).resolve().parents[1] / "scripts/sample_bill_text_reviews.py"
SPEC = importlib.util.spec_from_file_location("sample_bill_text_reviews", PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


@pytest.fixture
def candidate(tmp_path):
    root = tmp_path / "candidate"
    root.mkdir()
    data = {
        "bills_texts": (["bill_id", "date", "type", "formatted_text", "raw_text", "pdf", "xml"], [
            ["hr1-118", "2023-01-01", "Introduced", "https://example.test/one.htm", None, None, None],
            ["hr1-118", "2023-01-01", "Introduced", "https://example.test/two.htm", "text", None, None],
            ["s1-106", "1999-01-01", "Introduced", None, None, None, None],
            ["s2-110", "2007-01-01", "Unknown", None, None, None, None],
        ]),
        "bills_version_links": (["source_table", "source_row_key", "link_status", "govinfo_package_id"], [
            ["bills_texts", "hr1-118|2023-01-01|Introduced", "linked", "BILLS-118hr1ih"],
            ["bills_texts", "s2-110|2007-01-01|Unknown", "no_matching_govinfo_version", None],
        ]),
    }
    entries = []
    for name, (header, rows) in data.items():
        path = root / f"{name}.csv.gz"
        with gzip.open(path, "wt", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(header)
            writer.writerows([[r"\N" if value is None else value for value in row] for row in rows])
        entries.append({"name": name, "file": path.name, "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "row_count": len(rows)})
    manifest = root / "manifest.json"
    manifest.write_text(json.dumps({"tables": entries, "serialization": {"csv_null": r"\N", "format": "csv.gz", "header": True, "encoding": "UTF-8", "newline": "LF"}}))
    return manifest


def test_stratified_sample_includes_absent_candidates_and_occurrences(candidate, tmp_path):
    first = MODULE.sample(candidate, tmp_path / "first", 2)
    second = MODULE.sample(candidate, tmp_path / "second", 2)
    assert first["sampled_occurrences"] == 4 and first["review_cases"] == 8
    assert first["stratum_population"] == {"114_onward/linked": 2, "through_106/no_candidate": 1, "107_113/no_matching_govinfo_version": 1}
    assert (tmp_path / "first/review.csv").read_bytes() == (tmp_path / "second/review.csv").read_bytes()
    evidence = json.loads((tmp_path / "first/evidence.json").read_text())
    assert len({row["occurrence_id"] for row in evidence["cases"]}) == 4
    assert all(row["inclusion_probability"] == 1 for row in evidence["cases"])
    assert not first["independent_review_complete"]
    assert second["input_sha256"] == first["input_sha256"]


def test_per_stratum_bound_records_denominator(candidate, tmp_path):
    report = MODULE.sample(candidate, tmp_path / "sample", 1)
    assert report["sampled_occurrences"] == 3
    cases = json.loads((tmp_path / "sample/evidence.json").read_text())["cases"]
    assert next(row for row in cases if row["stratum"] == "114_onward/linked")["inclusion_probability"] == 0.5


def test_quality_strata_keep_present_prose_and_absence_separate(candidate, tmp_path):
    report = MODULE.sample(candidate, tmp_path / 'quality', 1, text_quality_strata=True, min_nonempty=1)
    assert report['prose_sample_count'] == 1
    assert report['sampled_occurrences'] == 4
    assert report['text_presence_population'] == {'prose_absent': 3, 'prose_present': 1}
    assert any('/prose_present' in name for name in report['stratum_sample'])


def test_prose_minimum_fails_closed(candidate, tmp_path):
    with pytest.raises(ValueError, match='require 2'):
        MODULE.sample(candidate, tmp_path / 'quality', 1, text_quality_strata=True, min_nonempty=2)


def test_laws_and_pre103_are_explicit_scopes():
    assert MODULE.source_scope({'bill_id': 'hr1-118', 'type': 'Public Law'}) == 'law_rendition'
    assert MODULE.source_scope({'bill_id': 'hr1-102', 'type': 'Engrossed'}) == 'pre_103'
    assert MODULE.text_presence({'raw_text': 'https://example.test/text.htm'}) == 'prose_absent'


def test_sampler_rejects_tamper_and_row_count_drift(candidate, tmp_path):
    manifest = json.loads(candidate.read_text())
    manifest["tables"][0]["row_count"] = 999
    candidate.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="row count"):
        MODULE.sample(candidate, tmp_path / "bad-count")
    manifest["tables"][0]["sha256"] = "0" * 64
    candidate.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="differs from manifest"):
        MODULE.sample(candidate, tmp_path / "bad-hash")


@pytest.mark.parametrize("count", [0, 21])
def test_sampler_requires_bounded_count(candidate, tmp_path, count):
    with pytest.raises(ValueError, match="per-stratum"):
        MODULE.sample(candidate, tmp_path / "bad", count)
