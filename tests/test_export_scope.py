import pytest

from bicam_collection.export_scope import primary_root_selected, member_ids_for_congress


def test_congress_scope_does_not_use_creation_or_update_dates():
    assert primary_root_selected("bills", {"congress": "119", "updated_at": None}, 119)
    assert not primary_root_selected("bills", {"congress": "118", "updated_at": "2026-09-10"}, 119)
    assert not primary_root_selected("bills", {"updated_at": "2026-09-10", "created_at": "2026-09-10"}, 119)


@pytest.mark.parametrize("family,column", [("amendments", "congress"),
    ("hearings", "congress"), ("committee_reports", "congress"),
    ("committee_prints", "congress"), ("committee_meetings", "congress"),
    ("nominations", "congress"), ("congresses", "congress_number")])
def test_family_identity_fields(family, column):
    assert primary_root_selected(family, {column: 119}, 119)
    assert not primary_root_selected(family, {column: 118}, 119)


def test_treaties_received_or_considered_in_target_congress():
    assert primary_root_selected("treaties", {"congress_received": 119}, 119)
    assert primary_root_selected("treaties", {"congress_received": 118, "congress_considered": 119}, 119)
    assert not primary_root_selected("treaties", {"congress_received": 118, "congress_considered": 118}, 119)


def test_members_use_service_terms_not_current_flag():
    serving = member_ids_for_congress([
        {"bioguide_id": "A000001", "congress": "119"},
        {"bioguide_id": "A000001", "congress": "118"},
        {"bioguide_id": "B000001", "congress": "118"},
    ], 119)
    assert serving == {"A000001"}
    assert primary_root_selected("members", {"bioguide_id": "A000001", "is_current_member": False}, 119, serving_members=serving)
    assert not primary_root_selected("members", {"bioguide_id": "B000001", "is_current_member": True}, 119, serving_members=serving)


def test_committee_scope_requires_evidence_and_unknown_family_fails():
    assert primary_root_selected("committees", {"committee_code": "hsag00"}, 119, involved_committees={"hsag00"})
    assert not primary_root_selected("committees", {"committee_code": "hsag00", "is_current": True}, 119)
    with pytest.raises(ValueError, match="undeclared family"):
        primary_root_selected("communications", {"congress": 119}, 119)
