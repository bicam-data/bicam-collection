import importlib.util
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyarrow")
scripts = Path(__file__).parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("directory_member_enrichment", scripts / "enrich_directory_member_export.py")
module = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
try:
    spec.loader.exec_module(module)
finally:
    sys.path.pop(0)


def test_term_delta_rejects_unrelated_fields_and_nonnull_overwrites():
    row = {"bioguide_id": "A000055", "congress": 119, "chamber": "house", "start_year": 2025,
           "biography": None, "title": None, "population": 5}
    change = {module.key(row): {"biography": "Source prose"}}
    module.verify_terms([row], [{**row, "biography": "Source prose"}], change)
    with pytest.raises(ValueError, match="unexpected member-term field"):
        module.verify_terms([row], [{**row, "biography": "Source prose", "title": "Invented"}], change)
    with pytest.raises(ValueError, match="only missing"):
        module.verify_terms([row], [{**row, "population": 6}], {module.key(row): {"population": 6}})
    with pytest.raises(ValueError, match="cardinality"):
        module.verify_terms([row], [], change)


def test_selection_rejects_damaged_prose_unreviewed_rules_and_duplicate_terms():
    record = {"status": "exact_name_state_chamber_and_directory_url", "prose_issues": [],
              "term_key": {"bioguide_id": "A000055", "congress": 119, "chamber": "house", "start_year": 2025},
              "proposed_fields": {"biography": "Source prose"}}
    assert len(module.selected_changes({"records": [record]})) == 1
    with pytest.raises(ValueError, match="damaged"):
        module.selected_changes({"records": [{**record, "proposed_fields": {"biography": "\ufffd"}}]})
    with pytest.raises(ValueError, match="unreviewed"):
        module.selected_changes({"records": [{**record, "status": "fuzzy"}]})
    with pytest.raises(ValueError, match="duplicate"):
        module.selected_changes({"records": [record, record]})
