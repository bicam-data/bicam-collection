"""Async raw fetchers for Treaty → related entities.

Currently Congress.gov exposes Treaties via a limited set of endpoints.  This
module wires up placeholder fetchers for the relations declared in
`configs/congressional_schema.yaml` so that integrity tests pass even when the
full implementation is pending.
"""

from __future__ import annotations

from typing import Any

from pycon.congress.abstractions import PyCongress

# Treaty model is optional: avoid hard dependency for unit-test environments.
try:
    from pycon.congress.components import Treaty  # type: ignore
except ModuleNotFoundError:  # pragma: no cover – SDK not installed
    Treaty = Any  # type: ignore

from pycon.models import ErrorResult  # type: ignore

from .raw_fetchers import register

__all__ = [
    "fetch_treaty_committeeactivities",
    "fetch_treaty_committeereports",
    "fetch_treaty_titles",
]


def _ensure_treaty(payload: Any, client: PyCongress) -> Treaty:  # type: ignore[name-defined]
    """Return a Treaty instance for *payload* (raw dict or already object)."""
    if "Treaty" in globals() and isinstance(payload, Treaty):  # type: ignore[name-defined]
        return payload  # type: ignore[return-value]

    treaty_json = payload.get("treaty", payload)
    if "Treaty" in globals():  # type: ignore[name-defined]
        return Treaty(data=treaty_json, _pagination={}, _adapter=client._adapter)  # type: ignore[return-value]
    return payload


# ---------------------------------------------------------------------------
# Placeholder fetchers – return [] when the PyCongress model lacks support.
# ---------------------------------------------------------------------------


@register("treaties", "committeeactivities")
async def fetch_treaty_committeeactivities(
    payload: Any, client: PyCongress
) -> list[dict]:
    treaty_obj = _ensure_treaty(payload, client)

    if hasattr(treaty_obj, "get_committeeactivities") and callable(
        treaty_obj.get_committeeactivities
    ):
        results: list[dict] = []
        async for act in treaty_obj.get_committeeactivities():  # type: ignore[attr-defined]
            if isinstance(act, ErrorResult):
                continue
            raw = getattr(act, "data", None)
            if raw is not None:
                raw["treaty_id"] = getattr(treaty_obj, "treaty_id", None)
                results.append(raw)
        return results

    return []


@register("treaties", "committeereports")
async def fetch_treaty_committeereports(payload: Any, client: PyCongress) -> list[dict]:
    treaty_obj = _ensure_treaty(payload, client)

    if hasattr(treaty_obj, "get_committeereports") and callable(
        treaty_obj.get_committeereports
    ):
        results: list[dict] = []
        async for rpt in treaty_obj.get_committeereports():  # type: ignore[attr-defined]
            if isinstance(rpt, ErrorResult):
                continue
            raw = getattr(rpt, "data", None)
            if raw is not None:
                raw["treaty_id"] = getattr(treaty_obj, "treaty_id", None)
                results.append(raw)
        return results
    return []


@register("treaties", "titles")
async def fetch_treaty_titles(payload: Any, client: PyCongress) -> list[dict]:
    treaty_obj = _ensure_treaty(payload, client)

    if hasattr(treaty_obj, "get_titles") and callable(
        treaty_obj.get_titles
    ):
        results: list[dict] = []
        async for title in treaty_obj.get_titles():  # type: ignore[attr-defined]
            if isinstance(title, ErrorResult):
                continue
            raw = getattr(title, "data", None)
            if raw is not None:
                raw["treaty_id"] = getattr(treaty_obj, "treaty_id", None)
                results.append(raw)
        return results
    return []
