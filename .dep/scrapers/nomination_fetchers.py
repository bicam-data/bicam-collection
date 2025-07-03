"""Async fetchers for Nomination → related endpoints.

Each function takes *(payload, client)* arguments and returns a list of raw
JSON dicts that can be inserted into the `bicam_raw_congressional.*` raw
staging tables.  The *payload* may already be a ``Nomination`` instance or the
raw JSON dict returned by the Congress.gov API – the helpers normalise to an
object instance first.  All emitted rows are tagged with ``nomination_id`` so
that loaders can link them back to the parent record.
"""

from __future__ import annotations

from typing import Any

from pycon.congress.abstractions import PyCongress
from pycon.congress.components import Nomination
from pycon.congress.subcomponents import (  # type: ignore
    CommitteeActivity,
    NominationPosition,
    Nominee,
)
from pycon.models import ErrorResult

from .raw_fetchers import register

__all__ = [
    "fetch_nomination_actions",
    "fetch_nomination_committeeactivities",
    "fetch_nomination_nominees",
    "fetch_nomination_positions",
    "fetch_nomination_hearings",
]


def _ensure_nomination(payload: Any, client: PyCongress) -> Nomination:
    """Return a :class:`Nomination` instance for *payload*."""
    if isinstance(payload, Nomination):
        return payload

    # Like other endpoints, the API may wrap under a top-level key
    nom_json = payload.get("nomination", payload)
    return Nomination(data=nom_json, _pagination={}, _adapter=client._adapter)


@register("nominations", "actions")
async def fetch_nomination_actions(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw action payloads for a nomination."""
    nom_obj = _ensure_nomination(payload, client)

    results: list[dict] = []
    async for action in nom_obj.get_actions():
        if isinstance(action, ErrorResult):
            continue
        raw = getattr(action, "data", None)
        if raw is not None:
            raw["nomination_id"] = nom_obj.nomination_id
            # Ensure URL propagated for lineage/debug like we do for bills etc.
            if "url" not in raw and getattr(action, "url", None):
                raw["url"] = action.url
            results.append(raw)
    return results


@register("nominations", "committeeactivities")
async def fetch_nomination_committeeactivities(
    payload: Any, client: PyCongress
) -> list[dict]:
    """Return raw committee‐activity payloads for a nomination."""
    nom_obj = _ensure_nomination(payload, client)

    results: list[dict] = []
    async for activity in nom_obj.get_committees():
        if isinstance(activity, ErrorResult):
            continue
        raw: dict | None
        if isinstance(activity, CommitteeActivity):
            raw = activity.data  # dataclass field
        else:
            raw = getattr(activity, "data", None)
        if raw is not None:
            raw["nomination_id"] = nom_obj.nomination_id
            results.append(raw)
    return results


@register("nominations", "nominees")
async def fetch_nomination_nominees(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw nominee payloads for a nomination."""
    nom_obj = _ensure_nomination(payload, client)

    results: list[dict] = []
    async for nominee in nom_obj.get_nominees():
        if isinstance(nominee, ErrorResult):
            continue
        raw: dict | None
        if isinstance(nominee, Nominee):
            raw = nominee.data
        else:
            raw = getattr(nominee, "data", None)
        if raw is not None:
            raw["nomination_id"] = nom_obj.nomination_id
            results.append(raw)
    return results


@register("nominations", "positions")
async def fetch_nomination_positions(
    payload: Any, client: PyCongress
) -> list[dict]:
    """Return raw Nominee‐position payloads for a nomination.

    Positions are available synchronously on the parent object (no additional
    API call required).  We therefore simply iterate over the pre‐parsed list.
    """
    nom_obj = _ensure_nomination(payload, client)

    results: list[dict] = []
    for pos in nom_obj.positions:
        if isinstance(pos, NominationPosition):
            raw = pos.data
        else:
            raw = getattr(pos, "data", None)
        if raw is not None:
            raw["nomination_id"] = nom_obj.nomination_id
            results.append(raw)
    return results


@register("nominations", "hearings")
async def fetch_nomination_hearings(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw hearing cross‐reference payloads for a nomination."""
    nom_obj = _ensure_nomination(payload, client)

    results: list[dict] = []
    async for hearing in nom_obj.get_hearings():
        if isinstance(hearing, ErrorResult):
            continue
        raw = getattr(hearing, "data", None)
        if raw is not None:
            raw["nomination_id"] = nom_obj.nomination_id
            results.append(raw)
    return results
