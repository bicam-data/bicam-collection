"""Fetch committee meetings referenced by Congress hearing records.

The source relationship is asymmetric: a hearing can advertise an
``associatedMeeting`` even when the committee-meeting list crawl has not
stored that event, while many meetings are not hearings. This bounded repair
follows only the explicit hearing-side URL and stores the payload in the
canonical ``committee-meetings`` category with hearing provenance.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import math
from pathlib import Path
import time
from typing import Any

import psycopg

from bicam_collection.client import fetch_metadata_values
from bicam_collection.config import Settings
from bicam_collection.runtime import build_client
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.urls import canonicalize_congress_api_url


CATEGORY = "committee-meetings"
NODE_TYPE = "committee_meeting_detail"


@dataclass(frozen=True, slots=True)
class MissingHearingMeeting:
    hearing_raw_id: str
    hearing_endpoint_url: str
    endpoint_url: str


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
) -> list[MissingHearingMeeting]:
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
        MissingHearingMeeting(
            hearing_raw_id=str(row[0]),
            hearing_endpoint_url=str(row[1]),
            endpoint_url=canonicalize_congress_api_url(str(row[2])),
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
    run_id = await store.start_run(CATEGORY, "targeted_hearing_committee_meeting_backfill")
    semaphore = asyncio.Semaphore(max(1, args.concurrency))
    succeeded = 0
    failed = 0
    counter_lock = asyncio.Lock()

    async def fetch(link: MissingHearingMeeting) -> None:
        nonlocal succeeded, failed
        started_at = time.perf_counter()
        endpoint_url = canonicalize_congress_api_url(link.endpoint_url)
        metadata_values: dict[str, Any] = {}
        try:
            async with semaphore:
                result = await client.get_json(
                    endpoint_url,
                    endpoint_name="congress-committee-meeting-from-hearing",
                    endpoint_priority=3,
                )
            metadata_values = fetch_metadata_values(result.metadata)
            response_ref = await store.upsert_response(
                endpoint_url=endpoint_url,
                source="congress",
                category=CATEGORY,
                node_type=NODE_TYPE,
                parent_id=link.hearing_raw_id,
                response=result.payload,
                latest_crawl_run_id=run_id,
            )
            await store.record_fetch(
                crawl_run_id=run_id,
                endpoint_url=endpoint_url,
                raw_response_id=response_ref.raw_response_id,
                raw_response_version_id=response_ref.raw_response_version_id,
                parent_endpoint_url=link.hearing_endpoint_url,
                node_type=NODE_TYPE,
                follow_mode="targeted",
                http_status=metadata_values["status_code"],
                outcome="fetched",
                duration_ms=metadata_values["duration_ms"],
                attempt_count=metadata_values["attempt_count"],
                rate_limit_attempts=metadata_values["rate_limit_attempts"],
                api_key_name=metadata_values["api_key_name"],
                pool_allocation=metadata_values["pool_allocation"],
            )
            async with counter_lock:
                succeeded += 1
        except Exception as exc:
            response = getattr(exc, "response", None)
            metadata = fetch_metadata_values(getattr(exc, "fetch_metadata", None))
            duration_ms = metadata["duration_ms"]
            if duration_ms is None:
                duration_ms = max(1, round((time.perf_counter() - started_at) * 1000))
            await store.record_failed_request(
                crawl_run_id=run_id,
                category=CATEGORY,
                node_type=NODE_TYPE,
                endpoint_url=endpoint_url,
                parent_endpoint_url=link.hearing_endpoint_url,
                http_status=getattr(response, "status_code", metadata["status_code"]),
                error=str(exc),
                duration_ms=duration_ms,
                attempt_count=metadata["attempt_count"],
                rate_limit_attempts=metadata["rate_limit_attempts"],
                api_key_name=metadata["api_key_name"],
                pool_allocation=metadata["pool_allocation"],
            )
            await store.record_fetch(
                crawl_run_id=run_id,
                endpoint_url=endpoint_url,
                raw_response_id=None,
                parent_endpoint_url=link.hearing_endpoint_url,
                node_type=NODE_TYPE,
                follow_mode="targeted",
                http_status=getattr(response, "status_code", metadata["status_code"]),
                outcome="failed",
                duration_ms=duration_ms,
                attempt_count=metadata["attempt_count"],
                rate_limit_attempts=metadata["rate_limit_attempts"],
                api_key_name=metadata["api_key_name"],
                pool_allocation=metadata["pool_allocation"],
            )
            async with counter_lock:
                failed += 1

    try:
        batch_size = max(1, args.batch_size)
        for start in range(0, len(links), batch_size):
            await asyncio.gather(*(fetch(link) for link in links[start : start + batch_size]))
        await store.finish_run(run_id, "completed", succeeded)
    except BaseException:
        await store.finish_run(run_id, "failed", succeeded)
        raise
    finally:
        await store.close()
    print(f"[hearing-meeting] completed run={run_id} fetched={succeeded} failed={failed}", flush=True)
    return succeeded, failed


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
