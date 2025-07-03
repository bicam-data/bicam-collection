"""Async raw fetchers for CommitteePrint relations."""

from __future__ import annotations

from typing import Any

from pycon.congress.abstractions import PyCongress
from pycon.congress.components import CommitteePrint
from pycon.models import ErrorResult

from .raw_fetchers import register

__all__ = ["fetch_committeeprint_texts"]


def _ensure_committeeprint(payload: Any, client: PyCongress) -> CommitteePrint:
    """Return a CommitteePrint instance for *payload*."""
    if isinstance(payload, CommitteePrint):
        return payload
    cp_json = payload.get("committeePrint", payload)
    # note: CommitteePrint constructor expects list sometimes; we just pass raw
    return CommitteePrint(data=cp_json, _pagination={}, _adapter=client._adapter)


@register("committeeprints", "texts")
async def fetch_committeeprint_texts(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw TextVersion payloads for a committee print."""
    cp = _ensure_committeeprint(payload, client)
    results: list[dict] = []

    # CommitteePrint.get_texts returns async iterator
    async for tv in cp.get_texts():
        if isinstance(tv, ErrorResult):
            continue
        raw = getattr(tv, "data", None)
        if raw is not None:
            raw["print_id"] = cp.print_id
            if "url" not in raw and getattr(tv, "url", None):
                raw["url"] = tv.url
            results.append(raw)
    return results
