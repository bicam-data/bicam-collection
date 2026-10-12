from collections import defaultdict
import importlib.util
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyarrow")
scripts = Path(__file__).parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("family_candidate", scripts / "export_congress_family_candidate.py")
module = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
try:
    spec.loader.exec_module(module)
finally:
    sys.path.pop(0)


@pytest.fixture
def candidate():
    c = module.Candidate.__new__(module.Candidate)
    c.congress = 119
    c.roots = {"bills": {"primary_key": ["bill_id"]}, "members": {"primary_key": ["bioguide_id"]},
               "committees": {"primary_key": ["committee_code"]}, "congresses": {"primary_key": ["congress_number"]}}
    c.primary = defaultdict(set, {"bills": {"hr1-119"}, "members": {"A000001"}, "committees": {"hsag00", "hsag03"}})
    c.references = defaultdict(set)
    c.indirect_keys = defaultdict(set, {"bills_reference_codes": {"code-1"}})
    return c


def test_member_history_is_congress_scoped(candidate):
    assert candidate.selected_child("members", "members_terms", {"bioguide_id": "A000001", "congress": "119"})
    assert not candidate.selected_child("members", "members_terms", {"bioguide_id": "A000001", "congress": "118"})
    candidate.references["members"].add("B000001")
    assert not candidate.selected_child("members", "members_terms", {"bioguide_id": "B000001", "congress": "118"})


def test_indirect_and_committee_children(candidate):
    assert candidate.selected_child("bills", "bills_reference_codes_sections", {"bill_code_id": "code-1"})
    assert not candidate.selected_child("bills", "bills_reference_codes_sections", {"bill_code_id": "code-2"})
    assert candidate.selected_child("committees", "committees_bills", {"committee_code": "hsag00", "bill_id": "hr1-119"})
    assert not candidate.selected_child("committees", "committees_bills", {"committee_code": "hsag00", "bill_id": "hr1-118"})


def test_references_do_not_become_primary_old_bills(candidate):
    relation = {"foreign_keys": {"related_bill_id": "bills.bill_id", "committee_code": "committees.committee_code"}}
    candidate.references_from(relation, {"related_bill_id": "hr1-118", "committee_code": "ssfi00", "congress": "119"})
    assert "hr1-118" in candidate.references["bills"]
    assert "hr1-118" not in candidate.primary["bills"]
    assert "ssfi00" in candidate.primary["committees"]
    assert "119" in candidate.references["congresses"]


def test_supplemental_children_follow_same_scope(candidate):
    candidate.rows = lambda name: iter([
        {"bill_id": "hr1-119", "text": "existing"},
        {"bill_id": "hr1-118", "text": "old"},
    ])
    candidate.child_additions = {"bills_actions": [
        {"bill_id": "hr1-119", "text": "supplement"},
        {"bill_id": "hr1-118", "text": "old supplement"},
    ]}
    assert list(candidate.child_rows("bills", "bills_actions")) == [
        {"bill_id": "hr1-119", "text": "existing"},
        {"bill_id": "hr1-119", "text": "supplement"},
    ]


def test_root_repair_requires_exact_normalized_previous_row(candidate):
    previous = {"amendment_id": "hamdt230-119", "congress": 119, "chamber": None}
    replacement = {**previous, "chamber": "house"}
    candidate.relations = {"amendments": {"columns": {"amendment_id": "text", "congress": "integer", "chamber": "text"}}}
    candidate.amendment_repairs = {"hamdt230-119": {"expected": previous, "row": replacement}}
    candidate.applied_amendments = set()
    assert candidate.repair_root("amendments", {**previous, "congress": "119"}) == replacement
    assert candidate.applied_amendments == {"hamdt230-119"}
    with pytest.raises(ValueError, match="no longer matches"):
        candidate.repair_root("amendments", {**previous, "chamber": "senate"})
