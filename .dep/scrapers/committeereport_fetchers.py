from typing import Any

from pycon.congress.abstractions import PyCongress
from pycon.congress.components import CommitteeReport
from pycon.models import ErrorResult

from .raw_fetchers import register


def _ensure_report(payload: Any, client: PyCongress) -> CommitteeReport:
    """Return a CommitteeReport instance for *payload* (raw dict or already object)."""
    if isinstance(payload, CommitteeReport):
        return payload
    report_json = payload.get("committeeReport", payload) or payload.get("committeeReports", payload)
    return CommitteeReport(data=report_json, _pagination={}, _adapter=client._adapter)

@register("committeereports", "texts")
async def fetch_committeereport_texts(payload: Any, client: PyCongress) -> list[dict]:
    report_obj = _ensure_report(payload, client)
    results: list[dict] = []
    async for text in report_obj.get_texts():
        if isinstance(text, ErrorResult):
            continue
        raw = getattr(text, "data", None)
        if raw is not None:
            raw["report_id"] = report_obj.report_id
            results.append(raw)
    return results




