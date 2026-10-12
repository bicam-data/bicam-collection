import importlib.util
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyarrow")
scripts = Path(__file__).parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("amendment_projection", scripts / "project_amendment_export_repairs.py")
module = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
try:
    spec.loader.exec_module(module)
finally:
    sys.path.pop(0)


def test_repair_requires_exact_incomplete_identity_and_keeps_existing_prose():
    old = {"amendment_id": "hamdt230-119", "congress": 119, "chamber": None,
           "updated_at": None, "description": "Previously observed prose"}
    source = {"type": "HAMDT", "number": "230", "congress": 119,
              "chamber": "House of Representatives", "updateDate": "2026-07-15T08:09:20Z",
              "amendedBill": {"type": "HR", "number": "1181", "congress": 119},
              "amendmentsToAmendment": {"count": 2}}
    result = module.project_root("hamdt230-119", source, old)
    assert result["description"] == old["description"]
    assert result["is_bill_amendment"] is True
    assert result["amendments_to_amendment_count"] == 0  # outgoing parents, not advertised children
    assert old["chamber"] is None
    with pytest.raises(ValueError):
        module.project_root("hamdt230-119", {**source, "congress": 118}, old)
    with pytest.raises(ValueError, match="placeholders"):
        module.project_root("hamdt230-119", source, {**old, "chamber": "house"})


def test_text_links_are_retained_without_extracting_bodies():
    result = module.project_text("samdt6656-119", {"date": "2026-07-13T04:00:00Z", "type": "Submitted",
        "formats": [{"type": "HTML", "url": "https://example.test/crec/text"},
                    {"type": "PDF", "url": "https://example.test/file.pdf"}]})
    assert result["raw_text"] is None
    assert result["date"] == "2026-07-13"
    assert result["html"] == "https://example.test/crec/text"
    assert module.project_text("samdt1-119", {})["pdf"] is None


def test_withdrawal_explains_count_difference_but_other_differences_do_not():
    source = {"cosponsors": {"count": 1, "countIncludingWithdrawnCosponsors": 2}}
    task = {"amendment_id": "samdt6656-119", "resource": "cosponsors",
            "advertised_count": 1, "advertised_count_matches": False}
    items = [{"sponsorshipWithdrawnDate": "2026-07-14"}, {}]
    assert module.source_count_review(source, task, items)["status"] == "explained_by_withdrawn_cosponsors"
    assert module.source_count_review(source, task, [{}, {}])["status"] == "unresolved_source_count_difference"


def test_case_normalization_and_reference_identity_are_explicit():
    result = module.canonical_row({"chamber": "text", "description": "text"},
                                  {"chamber": "House of Representatives", "description": "KEEP This."})
    assert result == {"chamber": "house", "description": "KEEP This."}
    assert module.entity_id({"type": "HR", "number": "123½", "congress": 119}, {"hr"}) == "hr123.5-119"
    with pytest.raises(ValueError):
        module.entity_id({"type": "HR", "number": "123", "congress": "unknown"}, {"hr"})


def test_dates_cast_in_utc_like_projection_session():
    assert module.date_only("2026-07-13T23:00:00-04:00") == "2026-07-14"
