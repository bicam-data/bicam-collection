import copy
import importlib.util
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyarrow")
scripts = Path(__file__).parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("directory_member_features", scripts / "audit_directory_member_features.py")
module = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
try:
    spec.loader.exec_module(module)
finally:
    sys.path.pop(0)

FIXTURE = Path(__file__).parent / "fixtures/directory_member_features/aderholt-119.txt"


def context():
    member = {"bioguide_id": "A000055", "direct_order_name": "Robert B. Aderholt", "normalized_name": "ROBERT B. ADERHOLT",
              "official_url": "https://aderholt.house.gov/"}
    term = {"bioguide_id": "A000055", "congress": 119, "chamber": "house", "state_name": "Alabama", "start_year": 2025,
            "biography": None, "population": None, "other_url": None}
    return [member], [term]


def extracted(text=None):
    return module.extract(text or FIXTURE.read_text(), {"ALABAMA": "Alabama"})


def test_real_biography_boundary_and_exact_match():
    records, failures, sections = extracted()
    assert not failures and len(records) == len(sections) == 1
    record = records[0]
    assert "Republican National Convention" in record["biography"]
    assert "Congressional Office" not in record["biography"] and "roster" not in record["biography"]
    lines = FIXTURE.read_text().splitlines()
    assert record["biography"] == "\n".join(lines[record["line_start"] - 1:record["line_end"]]).strip()
    result = module.match_records(records, *context())[0]
    assert result["status"] == "exact_name_state_chamber_and_directory_url"
    assert result["proposed_fields"]["population"] == 5024279
    assert result["proposed_fields"]["other_url"] == "https://aderholt.house.gov"
    assert "title" not in result["proposed_fields"]


@pytest.mark.parametrize("field,value", [("state_name", "Georgia"), ("chamber", "senate"), ("congress", 118)])
def test_context_mismatch_withholds_fields(field, value):
    members, terms = context()
    terms[0][field] = value
    result = module.match_records(extracted()[0], members, terms)[0]
    assert not result["proposed_fields"]


def test_conflicting_or_shared_url_never_suffices():
    members, terms = context()
    members[0]["official_url"] = "https://someoneelse.house.gov"
    assert module.match_records(extracted()[0], members, terms)[0]["status"] == "exact_name_state_chamber_and_directory_url"
    members, terms = context()
    members.append({**members[0], "bioguide_id": "A000056"})
    assert not module.match_records(extracted()[0], members, terms)[0]["proposed_fields"]


def test_damage_is_withheld_per_field_and_name_damage_blocks_identity():
    text = FIXTURE.read_text().replace("University", "Universit\ufffd", 1)
    result = module.match_records(extracted(text)[0], *context())[0]
    assert "biography" not in result["proposed_fields"] and "population" in result["proposed_fields"]
    damaged_name = FIXTURE.read_text().replace("ADERHOLT", "ADER\ufffdHOLT")
    assert not module.match_records(extracted(damaged_name)[0], *context())[0]["proposed_fields"]


def test_duplicate_biography_never_silently_selected():
    record = extracted()[0][0]
    matches = module.match_records([record, copy.deepcopy(record)], *context())
    assert all(r["status"] == "duplicate_source_member_requires_review" and not r["proposed_fields"] for r in matches)


def test_party_word_inside_prose_is_not_header_and_md_is_explicit_only():
    assert not module.NAME.match("Haleyville, 1992-96; delegate, Republican National Convention,")
    assert module.NAME.match("BETTY McCOLLUM, Democratic-Farmer-Labor Party, of Saint Paul, MN;")
    assert module.NAME.match("MICHAEL T. McCAUL, Republican of Austin, TX;")
    records = extracted()[0]
    records[0]["source_name"] += ", M.D."
    result = module.match_records(records, *context())[0]
    assert result["identity_name_rule"] == "remove_explicit_MD_credential" and result["proposed_fields"]
    records[0]["source_name"] = "ROB ADERHOLT"
    assert not module.match_records(records, *context())[0]["proposed_fields"]


def test_url_grammar_does_not_accept_lookalikes_or_query_data():
    assert module.official_url_key("https://www.aderholt.house.gov/") == "aderholt.house.gov"
    assert module.official_url_key("https://aderholt.house.gov.example.com") is None
    assert module.official_url_key("https://aderholt.house.gov?person=another") is None


def test_existing_fields_not_overwritten():
    members, terms = context()
    terms[0]["population"] = 123
    result = module.match_records(extracted()[0], members, terms)[0]
    assert "population" not in result["proposed_fields"]
    assert result["existing_value_conflicts"] == {"population": 123}


def test_line_wrapped_election_heading_preserves_prose():
    records, failures, _ = extracted(FIXTURE.read_text().replace("election history:", "election\nhistory:"))
    assert not failures and not records[0]["prose_issues"]
    assert "election\nhistory:" in records[0]["biography"]
