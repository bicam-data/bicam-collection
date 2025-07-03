"""Async fetchers for Hearing → related entities (committees, texts, associated meetings)."""

from __future__ import annotations

from typing import Any

from pycon.congress.abstractions import PyCongress
from pycon.congress.components import (
    Hearing,  # type: ignore  # just for type hints
)
from pycon.models import ErrorResult

from .raw_fetchers import register

__all__ = [
    "fetch_hearing_associatedmeetings",
]


def _ensure_hearing(payload: Any, client: PyCongress) -> Hearing:
    """Return a Hearing instance from payload or object."""
    if isinstance(payload, Hearing):
        return payload
    hear_json = payload.get("hearing", payload)
    return Hearing(data=hear_json, _pagination={}, _adapter=client._adapter)



@register("hearings", "associatedmeetings")
async def fetch_hearing_associatedmeetings(
    payload: Any, client: PyCongress
) -> list[dict]:
    """Return raw associated CommitteeMeeting payload for a hearing."""
    hear_obj = _ensure_hearing(payload, client)
    results: list[dict] = []
    async for meeting in hear_obj.get_associated_meeting():
        if isinstance(meeting, ErrorResult):
            continue
        raw = getattr(meeting, "data", None)
        if raw is not None:
            raw["hearing_id"] = hear_obj.hearing_id
            if "url" not in raw and getattr(meeting, "url", None):
                raw["url"] = meeting.url
            results.append(raw)
    return results
