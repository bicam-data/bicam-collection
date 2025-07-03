from typing import Any

from pycon.congress.abstractions import PyCongress
from pycon.congress.components import Amendment
from pycon.models import ErrorResult

from .raw_fetchers import register

__all__ = [
    "fetch_amendment_actions",
    "fetch_amendment_cosponsors",
    "fetch_amendment_texts",
]


def _ensure_amendment(payload: Any, client: PyCongress) -> Amendment:
    """Return an Amendment instance for *payload* (raw dict or already object)."""
    if isinstance(payload, Amendment):
        return payload

    # API may wrap under top-level "amendment" key just like other endpoints.
    amend_json = payload.get("amendment", payload)
    return Amendment(data=amend_json, _pagination={}, _adapter=client._adapter)


@register("amendments", "actions")
async def fetch_amendment_actions(payload: Any, client: PyCongress) -> list[dict]:
    """Yield raw action dicts for a given amendment."""
    amend_obj = _ensure_amendment(payload, client)

    results: list[dict] = []
    # Amendment class exposes ``get_actions`` for the iterator
    async for action in amend_obj.get_actions():
        if isinstance(action, ErrorResult):
            continue
        raw = getattr(action, "data", None)
        if raw is not None:
            raw["amendment_id"] = amend_obj.amendment_id
            if "url" not in raw and getattr(action, "url", None):
                raw["url"] = action.url
            results.append(raw)
    return results


@register("amendments", "cosponsors")
async def fetch_amendment_cosponsors(payload: Any, client: PyCongress) -> list[dict]:
    """Yield raw cosponsor dicts for a given amendment."""
    amend_obj = _ensure_amendment(payload, client)

    results: list[dict] = []
    async for member in amend_obj.get_cosponsors():
        if isinstance(member, ErrorResult):
            continue
        raw = getattr(member, "data", None)
        if raw is not None:
            raw["amendment_id"] = amend_obj.amendment_id
            if "url" not in raw and getattr(member, "url", None):
                raw["url"] = member.url
            results.append(raw)
    return results


@register("amendments", "texts")
async def fetch_amendment_texts(payload: Any, client: PyCongress) -> list[dict]:
    """Yield raw text-version dicts for a given amendment."""
    amend_obj = _ensure_amendment(payload, client)

    results: list[dict] = []
    async for text in amend_obj.get_texts():
        if isinstance(text, ErrorResult):
            continue
        raw = getattr(text, "data", None)
        if raw is not None:
            raw["amendment_id"] = amend_obj.amendment_id
            if "url" not in raw and getattr(text, "url", None):
                raw["url"] = text.url
            results.append(raw)
    return results
