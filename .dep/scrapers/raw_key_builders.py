"""Utility for deriving natural identifiers from raw API payloads.

This centralises the logic so we can tweak the rule for any data type in
one place – the scraper will fall back to these rules if a payload lacks
an explicit ID field.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

Builder = Callable[[dict[str, Any]], str | None]

_REGISTRY: dict[str, Builder] = {}


def register(data_type: str) -> Callable[[Builder], Builder]:
    """Decorator to register a builder for *data_type* (e.g. "bills")."""

    def _decorator(fn: Builder) -> Builder:
        _REGISTRY[data_type] = fn
        return fn

    return _decorator


def build(data_type: str, payload: dict[str, Any]) -> str | None:
    """Return the natural key for *payload* if we have a builder."""
    builder = _REGISTRY.get(data_type)
    if not builder:
        return None
    try:
        return builder(payload)
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Default builders
# ---------------------------------------------------------------------------


@register("bills")
def _bill_id(payload: dict[str, Any]) -> str | None:  # noqa: D401 – simple name
    # The API sometimes nests under a top-level key
    if "bill" in payload and isinstance(payload["bill"], dict):
        payload = payload["bill"]

    t = payload.get("type")
    number = payload.get("number")
    congress = payload.get("congress")
    if None in [t, number, congress]:
        return "ID ERROR"
    else:
        return f"{str(t).lower()}{number}-{congress}"


# ---------------------------------------------------------------------------
# Amendments
# ---------------------------------------------------------------------------


@register("amendments")
def _amendment_id(payload: dict[str, Any]) -> str | None:  # noqa: D401 – simple name
    """Return the canonical ``amendment_id`` for a raw Amendment payload.

    The identifier follows the same pattern used by the PyCongress ``Amendment``
    component – e.g. ``samd123-118`` – composed of the *type* (lower-cased),
    *number* and *congress* session.

    The API sometimes wraps the actual amendment under a top-level
    ``"amendment"`` key so we normalise for that first.
    """

    if "amendment" in payload and isinstance(payload["amendment"], dict):
        payload = payload["amendment"]

    a_type = payload.get("type")
    number = payload.get("number")
    congress = payload.get("congress")

    if None in [a_type, number, congress]:
        return "ID ERROR"
    else:
        return f"{str(a_type).lower()}{number}-{congress}"


# ---------------------------------------------------------------------------
# Members
# ---------------------------------------------------------------------------


@register("members")
def _member_bioguide_id(payload: dict[str, Any]) -> str | None:  # noqa: D401 simple helper
    """Return the canonical bioguide ID for a raw Member payload.

    The Congress.gov API embeds the identifier under the camel-cased
    ``bioguideId`` key.  Downstream loaders expect the snake-cased
    ``bioguide_id`` column, so we normalise the key and return the value
    verbatim.
    """

    if "member" in payload and isinstance(payload["member"], dict):
        nested = payload["member"]
    else:
        nested = payload

    bid = nested.get("bioguideId")
    if bid:
        return str(bid)
    return "ID ERROR"


# ---------------------------------------------------------------------------
# Committees
# ---------------------------------------------------------------------------


@register("committees")
def _committee_code(payload: dict[str, Any]) -> str | None:  # noqa: D401 simple helper
    """Return the canonical committee code for a raw Committee payload.

    The identifier is provided by the API as ``systemCode`` (camel-case).  We
    fall back to the snake-cased ``committee_code`` when present or, if the
    payload is wrapped under a ``committee`` key, look inside that dict.
    """

    if "committee" in payload and isinstance(payload["committee"], dict):
        nested = payload["committee"]
    else:
        nested = payload

    code = nested.get("systemCode")
    if code:
        return str(code)

    return "ID ERROR"


# ---------------------------------------------------------------------------
# Committee Reports
# ---------------------------------------------------------------------------


@register("committeereports")
def _committee_report_id(payload: dict[str, Any]) -> str | None:  # noqa: D401
    """Return canonical `report_id` for a Committee Report payload.

    Logic mirrors `CommitteeReport._parse_report_id` in *components.py* but with
    minimal dependencies.  Falls back to `citation` via `process_report_id` util.
    """

    if "committeeReports" in payload and isinstance(payload["committeeReports"], list):
        nested = payload["committeeReports"][0]
    else:
        nested = payload

    report_type = nested.get("reportType")
    number = nested.get("number")
    part = nested.get("part", '0')
    congress = nested.get("congress")

    if None in [report_type, number, congress]:
        citation = nested.get("citation")
        if citation:
            try:
                from pycon.utilis import (
                    process_report_id as _proc,
                )  # local import to avoid heavy deps at import time

                return _proc(citation)
            except Exception:
                return "ID ERROR"
    else:
        return f"{str(report_type).lower()[0]}rpt{number}-{part}-{congress}"



# ---------------------------------------------------------------------------
# Committee Prints
# ---------------------------------------------------------------------------


@register("committeeprints")
def _committee_print_id(payload: dict[str, Any]) -> str | None:
    """Return canonical `print_id` for a Committee Print payload.

    Mirrors CommitteePrint._parse_print_id logic: format is
    ``{chamber initial}prt{jacketnumber}-{congress}``.
    """

    if "committeePrint" in payload and isinstance(payload["committeePrint"], list):
        nested = payload["committeePrint"][0]
    else:
        nested = payload

    chamber = nested.get("chamber")
    jacket = nested.get("jacketNumber")
    congress = nested.get("congress")

    if None in [chamber, jacket, congress]:
        return "ID ERROR"
    else:
        return f"{str(chamber)[0].lower()}prt{jacket}-{congress}"




# ---------------------------------------------------------------------------
# Treaties
# ---------------------------------------------------------------------------


@register("treaties")
def _treaty_id(payload: dict[str, Any]) -> str | None:
    """Return canonical `treaty_id` (e.g., td117-2) for a Treaty payload."""

    if "treaty" in payload and isinstance(payload["treaty"], dict):
        nested = payload["treaty"]
    else:
        nested = payload

    # normalise fields
    congress = nested.get("congressReceived")
    number = nested.get("number")
    suffix = nested.get("suffix")

    if None in [congress, number]:
        return "ID ERROR"
    else:
        base = f"td{congress}-{number}"
        if suffix:
            return f"{base}{suffix}"
        return base




# ---------------------------------------------------------------------------
# Nominations
# ---------------------------------------------------------------------------


@register("nominations")
def _nomination_id(payload: dict[str, Any]) -> str | None:
    """Return canonical `nomination_id` for a raw Nomination payload.

    The identifier mirrors the logic used by :pyclass:`Nomination` from
    *pycon.congress.components* so that downstream loaders / SQL match the
    same primary-key.  The API sometimes nests the actual object under a
    top-level ``"nomination"`` key so we normalise the structure first.
    """

    # Unwrap
    if "nomination" in payload and isinstance(payload["nomination"], dict):
        payload = payload["nomination"]

    number = payload.get("number")
    congress = payload.get("congress")
    part = payload.get("partNumber", '00')

    if None in [number, congress]:
        return "ID ERROR"
    return f"{number}-{part}-{congress}"


# ---------------------------------------------------------------------------
# Hearings
# ---------------------------------------------------------------------------


@register("hearings")
def _hearing_id(payload: dict[str, Any]) -> str | None:
    """Return canonical `hearing_id` for a raw Hearing payload.

    Replicates Hearing._parse_hearing_id logic: combines *chamber*, *jacketNumber* and
    *congress* into the pattern "<chamber>hrg<jacket_number>-<congress>".
    """

    # Unwrap
    if "hearing" in payload and isinstance(payload["hearing"], dict):
        payload = payload["hearing"]

    jacket = payload.get("jacketNumber")
    chamber = payload.get("chamber")
    congress = payload.get("congress")
    if None in [chamber, jacket, congress]:
        return "ID ERROR"
    else:
        return f"{str(chamber)[0].lower()}hrg{jacket}-{congress}"


# ---------------------------------------------------------------------------
# Committee Meetings
# ---------------------------------------------------------------------------


@register("committeemeetings")
def _committeemeeting_id(payload: dict[str, Any]) -> str | None:
    """Return canonical `meeting_id` for a Committee Meeting payload."""

    if "committeeMeeting" in payload and isinstance(payload["committeeMeeting"], dict):
        payload = payload["committeeMeeting"]

    mid = payload.get("eventId")
    congress = payload.get("congress")
    if None in [mid, congress]:
        return "ID ERROR"
    return f"{mid}-{congress}"


# ---------------------------------------------------------------------------
# Congresses
# ---------------------------------------------------------------------------


@register("congresses")
def _congress_number(payload: dict[str, Any]) -> str | None:
    """Return canonical congress number as the natural key (e.g., 118)."""
    if "congress" in payload and isinstance(payload["congress"], dict):
        payload = payload["congress"]

    if "congress" in payload and isinstance(payload["congress"], dict):
        nested = payload["congress"]
    else:
        nested = payload

    num = nested.get("number")
    if num is None:
        name = nested.get("name")
        if isinstance(name, str) and name.strip():
            import re

            m = re.match(r"(\d+)", name)
            if m:
                num = m.group(1)
    if num is None:
        return "ID ERROR"
    return str(num)
