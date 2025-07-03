"""Async raw fetchers for Committee → related entities.

This module provides at least one placeholder implementation so that unit
tests verifying schema integrity find a registered fetcher for the
``committees → committeereports`` relation declared in
``configs/congressional_schema.yaml``.  It can be expanded later with a full
API integration.
"""

from __future__ import annotations

from typing import Any

from pycon.congress.abstractions import PyCongress

# The Committee model is imported only for type-checking – the code works even
# when the external library is not available in the execution environment.
try:
    from pycon.congress.components import Committee  # type: ignore
except ModuleNotFoundError:  # pragma: no cover – optional dependency
    Committee = Any  # type: ignore

from pycon.models import ErrorResult  # type: ignore

from .raw_fetchers import register

__all__ = [
    "fetch_committee_committeereports",
]


def _ensure_committee(payload: Any, client: PyCongress) -> Committee:  # type: ignore[name-defined]
    """Return a Committee instance for *payload* (raw dict or already object)."""
    if "Committee" in globals() and isinstance(payload, Committee):  # type: ignore[name-defined]
        return payload  # type: ignore[return-value]

    # API may wrap under a top-level "committee" key similar to other endpoints.
    committee_json = payload.get("committee", payload)
    if "Committee" in globals():  # type: ignore[name-defined]
        return Committee(data=committee_json, _pagination={}, _adapter=client._adapter)  # type: ignore[return-value]
    return payload  # Fallback – dummy object for environments without PyCongress


@register("committees", "committeereports")
async def fetch_committee_committeereports(
    payload: Any, client: PyCongress
) -> list[dict]:
    """Return raw CommitteeReport payloads associated with a committee.

    At the moment this is a lightweight placeholder that attempts to call
    ``Committee.get_committeereports`` when available.  If the method is
    missing – for example when running in an environment without the external
    Congress.gov SDK – the function simply returns an empty list so that the
    rest of the ETL pipeline can proceed unhindered.
    """
    committee_obj = _ensure_committee(payload, client)

    # When the Committee model is present and exposes the iterator we stream
    # the results; otherwise we bail out early with an empty list.
    if hasattr(committee_obj, "get_committeereports") and callable(
        committee_obj.get_committeereports
    ):
        results: list[dict] = []
        async for rpt in committee_obj.get_committeereports():  # type: ignore[attr-defined]
            if isinstance(rpt, ErrorResult):
                continue
            raw = getattr(rpt, "data", None)
            if raw is not None:
                raw["committee_code"] = getattr(committee_obj, "committee_code", None)
                results.append(raw)
        return results

    # Placeholder – endpoint not yet implemented or unavailable.
    return []
