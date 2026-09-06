"""Write a durable report for the reconciliation source-gap queue.

The report is safe to run from a scheduler.  Pushover support is intentionally
opt-in and inert until ``--notify-pushover`` is supplied together with
``PUSHOVER_TOKEN`` and ``PUSHOVER_USER``; no credentials are checked in or
configured by the project.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg

from bicam_collection.config import Settings


_CURRENT_QUEUE_CTE = """
with current_queue as (
    select
        queue.*,
        case
            when review_disposition = 'needs_source_refresh' then 'reprobe'
            when review_disposition = 'confirmed_source_gap'
             and (expires_at is null or expires_at <= now()) then 'reprobe'
            when review_disposition = 'confirmed_source_gap' then 'reprobe_after_expiry'
            when exception_status = 'open' and latest_fetch_outcome in ('failed', 'not_attempted')
                then 'fetch_missing_source'
            when exception_status = 'open' then 'review_exception'
            else 'no_action'
        end as current_next_action,
        case
            when review_disposition = 'needs_source_refresh' then 'open'
            when review_disposition = 'confirmed_source_gap'
             and (expires_at is null or expires_at <= now()) then 'due_for_reprobe'
            when review_disposition = 'confirmed_source_gap' then 'deferred_source_gap'
            when exception_status = 'open' then 'open'
            else 'closed'
        end as current_queue_status,
        case
            when review_disposition = 'confirmed_source_gap' then expires_at
            when exception_status = 'open' then coalesce(latest_fetch_at, now())
            else null
        end as current_next_action_at
    from public.fct_source_gap_queue as queue
)
"""
SUMMARY_SQL = _CURRENT_QUEUE_CTE + """
select current_queue_status, priority, count(*)::bigint
from current_queue
group by current_queue_status, priority
order by current_queue_status, priority
"""
ITEM_SQL = _CURRENT_QUEUE_CTE + """
select entity, canonical_id, missing_source, congress_number, priority,
       current_queue_status as queue_status,
       current_next_action as next_action,
       endpoint_url, latest_fetch_outcome, latest_fetch_http_status,
       current_next_action_at as next_action_at, review_reason
from current_queue
where current_queue_status in ('open', 'due_for_reprobe')
order by case priority when 'high' then 0 when 'normal' then 1 else 2 end,
         current_next_action_at nulls first, entity, canonical_id
limit %s
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("../reports/source_gap_queue"))
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--database-url")
    parser.add_argument(
        "--notify-pushover",
        action="store_true",
        help="Send one summary only when PUSHOVER_TOKEN and PUSHOVER_USER are explicitly configured.",
    )
    return parser.parse_args(argv)


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)


def _send_pushover(message: str) -> None:
    """Send an explicitly requested Pushover notification.

    This function is not called by normal scheduled reports.  Keeping the
    integration behind both a CLI switch and environment credentials makes it
    possible to enable later without wiring secrets into the crawler.
    """

    token = os.environ.get("PUSHOVER_TOKEN")
    user = os.environ.get("PUSHOVER_USER")
    if not token or not user:
        raise ValueError("--notify-pushover requires PUSHOVER_TOKEN and PUSHOVER_USER")
    body = urlencode({"token": token, "user": user, "message": message}).encode("utf-8")
    request = Request(
        "https://api.pushover.net/1/messages.json",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urlopen(request, timeout=20) as response:  # noqa: S310 - fixed Pushover URL
        if response.status >= 300:
            raise RuntimeError(f"Pushover returned HTTP {response.status}")


def render_markdown(report: dict[str, object]) -> str:
    summary = report["summary"]
    assert isinstance(summary, dict)
    lines = [
        "# BICAM source-gap queue",
        "",
        f"Generated: `{report['generated_at']}`",
        f"Total queued records: **{report['total_rows']}**",
        "",
        "| Status | Priority | Count |",
        "| --- | --- | ---: |",
    ]
    for key, count in sorted(summary.items()):
        status, priority = key.split("|", 1)
        lines.append(f"| {status} | {priority} | {count} |")
    lines.extend(["", "## Next actionable records", "", "| Entity | ID | Missing source | Priority | Action | Endpoint |", "| --- | --- | --- | --- | --- | --- |"])
    for item in report["items"]:
        assert isinstance(item, dict)
        endpoint = item.get("endpoint_url") or ""
        lines.append(
            f"| {item['entity']} | `{item['canonical_id']}` | {item['missing_source']} | "
            f"{item['priority']} | {item['next_action']} | {endpoint} |"
        )
    lines.append("")
    return "\n".join(lines)


def actionable_row_count(report: dict[str, object]) -> int:
    """Return rows that require operator/source action now."""

    summary = report.get("summary", {})
    if not isinstance(summary, dict):
        return 0
    return sum(
        int(count)
        for key, count in summary.items()
        if str(key).split("|", 1)[0] in {"open", "due_for_reprobe"}
    )


def build_report(database_url: str, limit: int = 100) -> dict[str, object]:
    limit = max(1, min(limit, 10_000))
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(SUMMARY_SQL).fetchall()
        summary = {f"{row[0]}|{row[1]}": int(row[2]) for row in rows}
        total_rows = sum(summary.values())
        item_rows = connection.execute(ITEM_SQL, (limit,)).fetchall()
    columns = [
        "entity", "canonical_id", "missing_source", "congress_number", "priority",
        "queue_status", "next_action", "endpoint_url", "latest_fetch_outcome",
        "latest_fetch_http_status", "next_action_at", "review_reason",
    ]
    items = [dict(zip(columns, row)) for row in item_rows]
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "total_rows": total_rows,
        "summary": summary,
        "items": items,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit < 1:
        raise SystemExit("--limit must be positive")
    database_url = args.database_url or Settings.from_env(Path(__file__).resolve().parents[1]).database_url
    report = build_report(database_url, args.limit)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    _write_atomic(args.output_dir / "latest.json", json.dumps(report, indent=2, default=_json_default) + "\n")
    _write_atomic(args.output_dir / "latest.md", render_markdown(report))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    _write_atomic(args.output_dir / f"{stamp}.json", json.dumps(report, indent=2, default=_json_default) + "\n")
    if args.notify_pushover:
        actionable = actionable_row_count(report)
        if actionable:
            _send_pushover(f"BICAM source-gap queue: {actionable} actionable records; see latest report.")
    print(f"wrote source-gap report rows={report['total_rows']} output={args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
