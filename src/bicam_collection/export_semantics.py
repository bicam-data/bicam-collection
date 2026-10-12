"""Explicit export-only semantics; never rewrite raw evidence or production rows.

Unknown categories are retained with an exception, not guessed or converted to
NULL. Identity corrections and matching require separate source evidence.
"""
from __future__ import annotations

from datetime import datetime, timezone
import re


CHAMBER_COLUMNS = frozenset({"chamber", "origin_chamber", "current_chamber"})
CHAMBERS = {
    "h": "house", "house": "house", "house of representatives": "house",
    "s": "senate", "senate": "senate", "j": "joint", "joint": "joint",
    "nochamber": "nochamber",
}
TYPE_FIELDS = {
    ("bills", "bill_type"): {"hr", "s", "hres", "sres", "hjres", "sjres", "hconres", "sconres"},
    ("amendments", "amendment_type"): {"hamdt", "samdt", "suamdt"},
    ("committee_reports", "report_type"): {"hrpt", "srpt", "erpt", "ccrpt"},
}
SEMANTIC_ENUMS = {
    "chamber": set(CHAMBERS.values()),
    "origin_chamber": set(CHAMBERS.values()),
    "current_chamber": set(CHAMBERS.values()),
    "bill_type": {"hr", "s", "hres", "sres", "hjres", "sjres", "hconres", "sconres"},
    "amendment_type": {"hamdt", "samdt", "suamdt"},
    "report_type": {"hrpt", "srpt", "erpt", "ccrpt"},
}
SEMANTIC_REGEXES = {
    "source_system_code": re.compile(r"^[0-9]+(?:\.0+)?$"),
    "calendar": re.compile(r"^[A-Z][0-9]{5}$"),
}


def semantic_pattern_valid(column: str, value: object) -> bool:
    """Check a value's semantic shape without requiring historical enumeration.

    Congress-119 legitimately introduces values absent from the 2025 archive
    (for example newer calendar numbers and joint hearings). Codes remain
    source text; this function only checks their declared structural pattern.
    """
    if value is None or value == "":
        return True
    if column in SEMANTIC_ENUMS:
        return str(value).strip().lower() in SEMANTIC_ENUMS[column]
    if column in SEMANTIC_REGEXES:
        return bool(SEMANTIC_REGEXES[column].fullmatch(str(value).strip()))
    if column in {"is_subcommittee", "is_current", "is_errata", "is_private", "is_law"}:
        return isinstance(value, bool) or str(value).strip().lower() in {"true", "false"}
    return True


def normalize_value(table: str, column: str, value: str | None) -> tuple[str | None, str | None]:
    """Return value and optional unresolved reason. Free text/IDs stay untouched."""
    if value is None or value == "":
        return value, None
    if column in CHAMBER_COLUMNS:
        # Casing is never a semantic inference.  Normalize it even when the
        # vocabulary is unknown, while retaining an exception so a reviewer
        # can decide whether the value belongs in the declared domain.
        lowered = value.strip().lower()
        result = CHAMBERS.get(lowered)
        return (result, None) if result else (lowered, "unreviewed_chamber_value")
    allowed = TYPE_FIELDS.get((table, column))
    if allowed is not None:
        result = value.strip().lower()
        return (result, None) if result in allowed else (value, "unreviewed_entity_type")
    return value, None


def parse_source_timestamp(value: str | None) -> datetime | None:
    if value is None or value == "":
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("source creation/update timestamp must include timezone")
    return parsed.astimezone(timezone.utc)


def classify_recency(row: dict, since: datetime) -> str:
    """Only explicit creation/update timestamps qualify, never crawl/event dates."""
    if since.tzinfo is None:
        raise ValueError("cutoff must include timezone")
    values = [parse_source_timestamp(row.get(k)) for k in ("created_at", "updated_at")]
    timestamps = [t for t in values if t is not None]
    if not timestamps:
        return "unknown_timestamp"
    return "qualifies" if any(t >= since for t in timestamps) else "before_cutoff"
