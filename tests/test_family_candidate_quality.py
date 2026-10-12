import importlib.util
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyarrow")
scripts = Path(__file__).parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("family_candidate_quality", scripts / "audit_family_candidate_quality.py")
module = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
try:
    spec.loader.exec_module(module)
finally:
    sys.path.pop(0)


def test_field_stats_separates_null_empty_and_semantic_domains():
    result = module.field_stats([
        {"chamber": "house", "raw_text": None, "title": ""},
        {"chamber": "joint", "raw_text": "", "title": None},
        {"chamber": None, "raw_text": "body", "title": "x"},
    ], ["chamber", "raw_text", "title"])
    assert result["rows"] == 3
    assert result["null_counts"] == {"chamber": 1, "raw_text": 1, "title": 1}
    assert result["empty_counts"] == {"raw_text": 1, "title": 1}
    assert result["semantic_domains"]["chamber"] == {"house": 1, "joint": 1}


def test_semantic_shape_records_case_without_normalizing_values():
    assert module.semantic_shape({"house": 2, "House": 1, "Joint": 3}) == {
        "house": {"case": "lower", "rows": 2},
        "House": {"case": "mixed", "rows": 1},
        "Joint": {"case": "mixed", "rows": 3},
    }


def test_semantic_patterns_allow_new_119_values_but_reject_bad_shapes():
    assert module.semantic_pattern_audit({"chamber": {"joint": 1}, "calendar": {"D00003": 1},
                                          "source_system_code": {"1": 1}, "is_current": {"True": 1}}) == {
        "chamber": {"invalid_values": [], "pattern_checked": True},
        "calendar": {"invalid_values": [], "pattern_checked": True},
        "source_system_code": {"invalid_values": [], "pattern_checked": True},
        "is_current": {"invalid_values": [], "pattern_checked": True},
    }
    result = module.semantic_pattern_audit({"chamber": {"joint-ish": 1}, "calendar": {"D3": 1},
                                            "source_system_code": {"nine": 1}})
    assert result["chamber"]["invalid_values"] == ["joint-ish"]
    assert result["calendar"]["invalid_values"] == ["D3"]
    assert result["source_system_code"]["invalid_values"] == ["nine"]
