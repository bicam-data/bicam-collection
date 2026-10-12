import importlib.util
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyarrow")
scripts = Path(__file__).parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("member_icpsr_enrichment", scripts / "enrich_member_icpsr_export.py")
module = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
try:
    spec.loader.exec_module(module)
finally:
    sys.path.pop(0)


def test_default_enrichment_leaves_conflicting_role_ids_unresolved():
    proposals = [
        {"bioguide_id": "A000001", "current_icpsr": None, "status": "exact_bioguide_congress_chamber_state",
         "published_comparison": "agrees", "proposed_icpsr": "12345"},
        {"bioguide_id": "V000133", "current_icpsr": None, "status": "exact_bioguide_congress_chamber_state",
         "published_comparison": "differs_requires_review", "proposed_icpsr": "91980", "published_icpsr": "21980"},
    ]
    changes, unresolved = module.selected_enrichments(proposals)
    assert changes == {"A000001": "12345"} and len(unresolved) == 1
    with pytest.raises(ValueError, match="user direction"):
        module.selected_enrichments(proposals, "published_bicam")
    changes, unresolved = module.selected_enrichments(proposals, "published_bicam", "test explicit user selection")
    assert changes["V000133"] == "21980" and not unresolved


def test_member_delta_rejects_unrelated_change_or_missing_member():
    before = [{"bioguide_id": "A000001", "icpsr": None, "name": "Original"}]
    after = [{"bioguide_id": "A000001", "icpsr": "12345", "name": "Original"}]
    module.verify_member_changes(before, after, {"A000001": "12345"})
    with pytest.raises(ValueError, match="unexpected member field"):
        module.verify_member_changes(before, [{**after[0], "name": "Modified"}], {"A000001": "12345"})
    with pytest.raises(ValueError, match="cardinality"):
        module.verify_member_changes(before, [], {"A000001": "12345"})


def test_identifier_collision_is_not_allowed():
    rows = [{"bioguide_id": identity, "current_icpsr": None, "status": "exact_bioguide_congress_chamber_state",
             "published_comparison": "not_available", "proposed_icpsr": "12345"} for identity in ("A000001", "B000001")]
    with pytest.raises(ValueError, match="conflate"):
        module.selected_enrichments(rows)
