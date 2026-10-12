import importlib.util
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyarrow")
scripts = Path(__file__).parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("hearing_child_projection", scripts / "project_hearing_export_children.py")
module = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
try:
    spec.loader.exec_module(module)
finally:
    sys.path.pop(0)


@pytest.fixture
def members_and_terms():
    return ({"V000133": {"direct_order_name": "Jefferson Van Drew"}}, [
        {"bioguide_id": "V000133", "congress": 119, "chamber": "house", "state_code": "NJ", "state_name": "New Jersey"}])


def test_missing_member_id_requires_unique_exact_name_and_full_context(members_and_terms):
    source = {"name": [{"parsed": "Jefferson Van Drew of New Jersey"}], "chamber": "H", "state": "NJ", "congress": "119"}
    assert module.resolve_member(source, *members_and_terms) == ("V000133", "unique_exact_full_name_state_congress_chamber")
    for changed in ({"congress": "118"}, {"state": "NY"}, {"chamber": "S"},
                    {"name": [{"parsed": "Jeff Van Drew of New Jersey"}]}):
        assert module.resolve_member({**source, **changed}, *members_and_terms)[0] is None
    members, terms = members_and_terms
    members = {**members, "V999999": members["V000133"]}
    terms = [*terms, {**terms[0], "bioguide_id": "V999999"}]
    assert module.resolve_member(source, members, terms)[0] is None


def test_valid_bioguide_with_contradictory_state_does_not_link(members_and_terms):
    source = {"bioGuideId": "V000133", "state": "NY", "congress": "119", "chamber": "H"}
    assert module.resolve_member(source, *members_and_terms) == (None, "source_bioguide_term_contradiction")
    assert module.resolve_member({**source, "state": "NJ"}, *members_and_terms)[0] == "V000133"


def test_committee_o_repair_requires_known_target_and_keeps_valid_o():
    assert module.resolve_committee("hsgo00", {"hsgo00"}) == ("hsgo00", "exact_canonical")
    assert module.resolve_committee("hsg0oo", {"hsg000"}) == ("hsg000", "verified_o_to_zero")
    assert module.resolve_committee("hszzoo", {"hsgo00"})[0] is None


def test_nested_member_arrays_are_not_skipped():
    payload = {"members": [{"bioGuideId": "V000133"}], "nested": [{"members": [{"bioGuideId": "A000001"}]}]}
    assert [p for p, _ in module.member_objects(payload)] == ["$.members[0]", "$.nested[0].members[0]"]


@pytest.mark.parametrize("payload", [{"api_key": "secret"}, {"nested": [{"url": "https://example.test/?api_key=secret"}]}])
def test_source_capture_rejects_credentials(payload):
    with pytest.raises(ValueError, match="credential"):
        module.assert_no_credentials(payload)
