"""Congress-based export scope; dates do not substitute for Congress identity."""
from __future__ import annotations

ROOT_CONGRESS_COLUMNS = {
    "bills": ("congress",), "amendments": ("congress",),
    "committee_meetings": ("congress",), "committee_prints": ("congress",),
    "committee_reports": ("congress",), "congresses": ("congress_number",),
    "hearings": ("congress",), "nominations": ("congress",),
    "treaties": ("congress_received", "congress_considered"),
}


def in_congress(value, congress: int) -> bool:
    return value is not None and str(value) == str(congress)


def primary_root_selected(family: str, row: dict, congress: int,
                          *, serving_members=frozenset(), involved_committees=frozenset()) -> bool:
    if family == "members":
        return row.get("bioguide_id") in serving_members
    if family == "committees":
        return row.get("committee_code") in involved_committees
    if family not in ROOT_CONGRESS_COLUMNS:
        raise ValueError(f"undeclared family scope: {family}")
    return any(in_congress(row.get(field), congress) for field in ROOT_CONGRESS_COLUMNS[family])


def member_ids_for_congress(term_rows, congress: int) -> set[str]:
    return {r["bioguide_id"] for r in term_rows
            if r.get("bioguide_id") and in_congress(r.get("congress"), congress)}
