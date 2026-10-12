"""Fetch committee meetings that hearing records point to (``associatedMeeting``).

Stored under committee-meetings with the hearing as parent.
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
    category="committee-meetings",
    node_type="committee_meeting_detail",
    endpoint_name="congress-committee-meeting-from-hearing",
    endpoint_priority=3,
    run_mode="targeted_hearing_committee_meeting_backfill",
    record_failed_fetches=True,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch committee meetings explicitly referenced by hearing details."
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
    if not math.isfinite(retry_404_after_hours) or retry_404_after_hours <= 0:
        raise ValueError("retry_404_after_hours must be positive and finite")

    existing_clause = "" if refresh else """
        and not exists (
            select 1
            from raw_responses as existing
            where existing.source = 'congress'
              and existing.category = 'committee-meetings'
              and existing.node_type = 'committee_meeting_detail'
              and regexp_replace(existing.endpoint_url, '[?].*$', '') = links.endpoint_path
        )
        and not exists (
            -- failed_requests is the compact failure ledger.  Looking up the
            -- same evidence in crawl_run_fetches forced an expression scan of
            -- the multi-gigabyte fetch history once per hearing link.
            select 1
            from failed_requests as prior_failure
            where regexp_replace(prior_failure.endpoint_url, '[?].*$', '') = links.endpoint_path
              and prior_failure.category = 'committee-meetings'
              and prior_failure.node_type = 'committee_meeting_detail'
              and prior_failure.http_status = 404
              and prior_failure.failed_at >= now() - (%s * interval '1 hour')
        )
    """
    query = f"""
        with links as (
            select distinct on (regexp_replace(hearing.response->'hearing'->'associatedMeeting'->>'url', '[?].*$', ''))
                hearing.id::text as hearing_raw_id,
                hearing.endpoint_url as hearing_endpoint_url,
                hearing.response->'hearing'->'associatedMeeting'->>'url' as endpoint_url,
                regexp_replace(
                    hearing.response->'hearing'->'associatedMeeting'->>'url',
                    '[?].*$',
                    ''
                ) as endpoint_path
            from raw_responses as hearing
            where hearing.source = 'congress'
              and hearing.category = 'hearings'
              and hearing.node_type = 'hearing_detail'
              and hearing.response->'hearing'->'associatedMeeting'->>'url' is not null
            order by
                regexp_replace(hearing.response->'hearing'->'associatedMeeting'->>'url', '[?].*$', ''),
                hearing.fetched_at desc
        )
        select hearing_raw_id, hearing_endpoint_url, endpoint_path
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
    print(f"[hearing-meeting] candidates={len(links)}", flush=True)
    if not links:
        return 0, 0

    spec = load_spec(project_root / "specs/committee-meetings.yaml")
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
        f"[hearing-meeting] completed run={outcome.run_id} "
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
