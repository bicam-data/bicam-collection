"""Fetch hearings that committee-meeting records point to (``hearingTranscript``).

Stored under hearings with the meeting as parent; many hearings have no meeting.
"""

from __future__ import annotations

import argparse
import asyncio
import math
from pathlib import Path

import psycopg

from bicam_collection.config import Settings
from bicam_collection.runtime import build_client
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.targeted_backfill import (
    TargetedCandidate,
    TargetedFamily,
    run_targeted_backfill,
)
from bicam_collection.urls import canonicalize_congress_api_url


FAMILY = TargetedFamily(
    category="hearings",
    node_type="hearing_detail",
    endpoint_name="congress-hearing-from-committee-meeting",
    endpoint_priority=3,
    run_mode="targeted_committee_meeting_hearing_backfill",
    record_failed_fetches=True,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch hearing details referenced by committee meetings."
    )
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--request-interval", type=float, default=1.5)
    parser.add_argument("--key-wait-timeout", type=float, default=600.0)
    parser.add_argument("--active-key-budget", type=int, default=2)
    parser.add_argument(
        "--retry-404-after-hours",
        type=float,
        default=168.0,
        help="Retry a recently failed 404 only after this many hours (default: 168).",
    )
    return parser.parse_args(argv)


def load_missing(
    *, database_url: str, limit: int, refresh: bool = False, retry_404_after_hours: float = 168.0
) -> list[TargetedCandidate]:
    """Return distinct meeting transcript links with no stored hearing detail."""

    if not math.isfinite(retry_404_after_hours) or retry_404_after_hours <= 0:
        raise ValueError("retry_404_after_hours must be positive and finite")

    # Compare paths; older payloads omit format=json.
    existing_clause = "" if refresh else """
        and not exists (
            select 1
            from raw_responses as existing
            where existing.source = 'congress'
              and existing.category = 'hearings'
              and existing.node_type = 'hearing_detail'
              and existing.endpoint_url in (links.endpoint_url, links.endpoint_path)
        )
        -- A missing hearing can be a legitimate source gap.  Do not spend
        -- quota retrying a recent 404 on every supervisor pass; transient
        -- 500/503/429 failures remain eligible immediately.
        and not exists (
            select 1
            from (
                -- Keep this correlated to the small link set so PostgreSQL
                -- can use the endpoint-url index.  A global DISTINCT ON over
                -- crawl_run_fetches scans tens of millions of telemetry rows.
                select prior.http_status, prior.fetched_at
                from crawl_run_fetches as prior
                where prior.endpoint_url in (links.endpoint_url, links.endpoint_path)
                order by prior.fetched_at desc
                limit 1
            ) as prior_failure
            where prior_failure.http_status = 404
              and prior_failure.fetched_at >= now() - (%s * interval '1 hour')
        )
    """
    query = f"""
        with links as (
            select distinct on (regexp_replace(item->>'url', '[?].*$', ''))
                meetings.id::text as meeting_raw_id,
                meetings.endpoint_url as meeting_endpoint_url,
                item->>'url' as endpoint_url,
                regexp_replace(item->>'url', '[?].*$', '') as endpoint_path
            from raw_responses as meetings
            cross join lateral jsonb_array_elements(
                coalesce(meetings.response->'committeeMeeting'->'hearingTranscript', '[]'::jsonb)
            ) as item
            where meetings.source = 'congress'
              and meetings.category = 'committee-meetings'
              and meetings.node_type = 'committee_meeting_detail'
              and item->>'url' is not null
            order by regexp_replace(item->>'url', '[?].*$', ''), meetings.fetched_at desc
        )
        select meeting_raw_id, meeting_endpoint_url, endpoint_path
        from links
        where true
          {existing_clause}
        order by endpoint_path
        limit %s
    """
    with psycopg.connect(database_url) as connection:
        params: tuple[float | int, ...]
        if refresh:
            params = (max(1, limit),)
        else:
            params = (retry_404_after_hours, max(1, limit))
        rows = connection.execute(query, params).fetchall()
    return [
        TargetedCandidate(
            endpoint_url=canonicalize_congress_api_url(str(row[2])),
            parent_id=str(row[0]),
            parent_endpoint_url=str(row[1]),
        )
        for row in rows
    ]


async def run(args: argparse.Namespace, *, project_root: Path) -> tuple[int, int]:
    settings = Settings.from_env(project_root)
    links = load_missing(
        database_url=settings.database_url,
        limit=args.limit,
        retry_404_after_hours=args.retry_404_after_hours,
    )
    print(f"[meeting-hearing] candidates={len(links)}", flush=True)
    if not links:
        return 0, 0

    spec = load_spec(project_root / "specs/hearings.yaml")
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=max(0.0, args.request_interval),
        active_key_budget=max(1, args.active_key_budget),
    )
    store = AsyncPostgresStore(settings.database_url)
    outcome = await run_targeted_backfill(
        FAMILY,
        links,
        client=client,
        store=store,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
    )
    print(
        f"[meeting-hearing] completed run={outcome.run_id} "
        f"fetched={outcome.fetched} failed={outcome.failed}",
        flush=True,
    )
    return outcome.fetched, outcome.failed


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit < 1 or args.concurrency < 1 or args.batch_size < 1 or args.active_key_budget < 1:
        raise SystemExit("limit, concurrency, batch-size, and active-key-budget must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0:
        raise SystemExit("request-interval must be non-negative and key-wait-timeout must be positive")
    if not math.isfinite(args.retry_404_after_hours) or args.retry_404_after_hours <= 0:
        raise SystemExit("retry-404-after-hours must be positive and finite")
    asyncio.run(run(args, project_root=Path(__file__).resolve().parents[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
