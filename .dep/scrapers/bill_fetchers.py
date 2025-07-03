from typing import Any

from pycon.congress.abstractions import PyCongress
from pycon.congress.components import Bill
from pycon.models import ErrorResult

from .raw_fetchers import register


def _ensure_bill(payload: Any, client: PyCongress) -> Bill:
    """Return a Bill instance for *payload* (raw dict or already object)."""
    if isinstance(payload, Bill):
        return payload

    # API may wrap under top-level "bill" key just like other endpoints.
    bill_json = payload.get("bill", payload)
    return Bill(data=bill_json, _pagination={}, _adapter=client._adapter)


@register("bills", "actions")
async def fetch_bill_actions(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw action payloads for a bill.

    The *payload* argument may be either a Bill instance or the raw JSON dict
    returned by the API.  The *client* is the PyCongress instance already
    authenticated for the scraper run.
    """
    bill_obj = _ensure_bill(payload, client)

    results: list[dict] = []
    async for action in bill_obj.get_actions():
        if isinstance(action, ErrorResult):
            continue
        # Store the raw JSON payload (attribute `.data` holds it)
        raw = getattr(action, "data", None)
        if raw is not None:
            raw["bill_id"] = bill_obj.bill_id  # link to parent
            results.append(raw)

    return results


@register("bills", "cosponsors")
async def fetch_bill_cosponsors(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw cosponsor payloads for a bill."""
    bill_obj = _ensure_bill(payload, client)

    results: list[dict] = []
    async for cosponsor in bill_obj.get_cosponsors():
        if isinstance(cosponsor, ErrorResult):
            continue
        raw = getattr(cosponsor, "data", None)
        if raw is not None:
            raw["bill_id"] = bill_obj.bill_id
            results.append(raw)
    return results


@register("bills", "relatedbills")
@register("bills", "billrelations")
async def fetch_bill_relations(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw related‐bill payloads for a bill."""
    bill_obj = _ensure_bill(payload, client)

    results: list[dict] = []
    async for rel in bill_obj.get_billrelations():
        if isinstance(rel, ErrorResult):
            continue
        raw = getattr(rel, "data", None)
        if raw is not None:
            raw["bill_id"] = bill_obj.bill_id
            results.append(raw)
    return results


@register("bills", "texts")
async def fetch_bill_texts(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw text-version payloads for a bill."""
    bill_obj = _ensure_bill(payload, client)

    results: list[dict] = []
    async for text in bill_obj.get_texts():
        if isinstance(text, ErrorResult):
            continue
        raw = getattr(text, "data", None)
        if raw is not None:
            raw["bill_id"] = bill_obj.bill_id
            results.append(raw)
    return results


@register("bills", "titles")
async def fetch_bill_titles(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw title payloads for a bill."""
    bill_obj = _ensure_bill(payload, client)

    results: list[dict] = []
    async for title in bill_obj.get_titles():
        if isinstance(title, ErrorResult):
            continue
        raw = getattr(title, "data", None)
        if raw is not None:
            raw["bill_id"] = bill_obj.bill_id
            results.append(raw)
    return results


@register("bills", "summaries")
async def fetch_bill_summaries(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw summary payloads for a bill."""
    bill_obj = _ensure_bill(payload, client)

    results: list[dict] = []
    async for summary in bill_obj.get_summaries():
        if isinstance(summary, ErrorResult):
            continue
        raw = getattr(summary, "data", None)
        if raw is not None:
            raw["bill_id"] = bill_obj.bill_id
            results.append(raw)
    return results


@register("bills", "subjects")
async def fetch_bill_subjects(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw subject payloads for a bill."""
    bill_obj = _ensure_bill(payload, client)

    results: list[dict] = []
    async for subject in bill_obj.get_subjects():
        if isinstance(subject, ErrorResult):
            continue
        raw = getattr(subject, "data", None)
        if raw is not None:
            raw["bill_id"] = bill_obj.bill_id
            results.append(raw)
    return results


@register("bills", "committees")
async def fetch_bill_committees(payload: Any, client: PyCongress) -> list[dict]:
    """Return raw committee payloads for a bill."""
    bill_obj = _ensure_bill(payload, client)

    results: list[dict] = []
    async for committee in bill_obj.get_committees():
        if isinstance(committee, ErrorResult):
            continue
        raw = getattr(committee, "data", None)
        if raw is not None:
            raw["bill_id"] = bill_obj.bill_id
            results.append(raw)
    return results


@register("bills", "committeeactivities")
async def fetch_bill_committeeactivities(
    payload: Any, client: PyCongress
) -> list[dict]:
    """Return raw committee‐activity payloads for a bill.

    This is currently a lightweight placeholder implementation to satisfy
    schema integrity tests.  If the upstream ``Bill`` component exposes a
    ``get_committeeactivities`` coroutine the results are streamed and each
    payload is tagged with ``bill_id``.  Otherwise the function returns an
    empty list so that downstream loaders can safely ignore the relation
    until fully implemented.
    """
    bill_obj = _ensure_bill(payload, client)

    # Prefer the concrete async iterator when available; fall back to no-op.
    if hasattr(bill_obj, "get_committeeactivities") and callable(
        bill_obj.get_committeeactivities
    ):
        results: list[dict] = []
        async for ca in bill_obj.get_committeeactivities():
            if isinstance(ca, ErrorResult):
                continue
            raw = getattr(ca, "data", None)
            if raw is not None:
                raw["bill_id"] = bill_obj.bill_id
                results.append(raw)
        return results

    # Placeholder – endpoint not yet implemented in PyCongress
    return []
