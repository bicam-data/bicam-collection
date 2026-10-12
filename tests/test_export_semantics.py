from datetime import datetime, timezone

import pytest

from bicam_collection.export_semantics import classify_recency, normalize_value


@pytest.mark.parametrize("raw,expected", [("House", "house"), ("House of Representatives", "house"),
    (" SENATE ", "senate"), ("Joint", "joint"), ("NoChamber", "nochamber"), (None, None), ("", "")])
def test_declared_chamber_aliases(raw, expected):
    assert normalize_value("members_terms", "chamber", raw) == (expected, None)


def test_unknown_chamber_is_not_guessed_joint_or_null():
    assert normalize_value("hearings", "chamber", "UNKNOWN") == ("unknown", "unreviewed_chamber_value")


def test_only_explicit_fields_change():
    assert normalize_value("committee_reports", "report_type", "HRPT") == ("hrpt", None)
    assert normalize_value("bills", "bill_type", "cen_doc_h")[1] == "unreviewed_entity_type"
    for field, raw in [("title", "HOUSE Rules"), ("bioguide_id", "O000167"),
                       ("committee_code", "hsOO00"), ("state", "New York"), ("raw_text", "Example")]:
        assert normalize_value("members", field, raw) == (raw, None)


@pytest.mark.parametrize("row,expected", [
    ({"updated_at": "2025-01-01T00:00:00Z"}, "qualifies"),
    ({"created_at": "2025-01-01T01:00:00+01:00", "updated_at": "2024-01-01T00:00:00Z"}, "qualifies"),
    ({"updated_at": "2025-01-01T00:00:00+01:00"}, "before_cutoff"),
    ({"updated_at": None, "created_at": None, "introduced_at": "2026-01-01", "fetched_at": "2026-01-01"}, "unknown_timestamp"),
    ({"updated_at": "", "created_at": "2026-01-01T00:00:00Z"}, "qualifies"),
])
def test_explicit_source_dates_only(row, expected):
    assert classify_recency(row, datetime(2025, 1, 1, tzinfo=timezone.utc)) == expected


def test_naive_and_invalid_timestamps_fail_instead_of_excluding_rows():
    cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for raw in ("2025-01-01", "not-a-date"):
        with pytest.raises(ValueError):
            classify_recency({"updated_at": raw}, cutoff)
