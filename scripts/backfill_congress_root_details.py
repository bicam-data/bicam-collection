"""Fetch Congress.gov detail URLs that are present in stored list payloads but missing.

Historical list crawls can contain more than one URL spelling or pagination
variant.  A descendant backfill may therefore inspect every stored list page
without ever putting a genuinely missing detail URL on the network queue.  This
repair enumerates the list items directly, compares URL paths against current
raw responses, and fetches only the missing detail records.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
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


ROOTS: dict[str, tuple[str, str, str]] = {
    "bills": ("bill_list", "bills", "bill_detail"),
    "amendments": ("amendment_list", "amendments", "amendment_detail"),
}


@dataclass(frozen=True, slots=True)
class RootDetail:
    endpoint_url: str
    parent_raw_id: str
    parent_endpoint_url: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--category", choices=sorted(ROOTS), required=True)
    parser.add_argument("--congress-start", type=int, required=True)
    parser.add_argument("--congress-end", type=int, required=True)
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=256,
        help="Maximum number of detail tasks scheduled at once",
    )
    parser.add_argument("--request-interval", type=float, default=1.10)
    parser.add_argument("--key-wait-timeout", type=float, default=3600.0)
    parser.add_argument("--active-key-budget", type=int, default=2)
    parser.add_argument(
        "--max-source-error-attempts",
        type=int,
        default=3,
        help=(
            "Stop re-queuing a detail URL after this many recorded source-side "
            "404/500 failures; transient rate-limit failures remain retryable."
        ),
    )
    return parser.parse_args(argv)


def _path_pattern(category: str, start: int, end: int) -> str:
    values = "|".join(str(value) for value in range(start, end + 1))
    return rf"/{category[:-1]}/({values})/"


def load_missing(
    *,
    database_url: str,
    category: str,
    congress_start: int,
    congress_end: int,
    limit: int,
    max_source_error_attempts: int = 3,
) -> list[RootDetail]:
    list_node, item_key, detail_node = ROOTS[category]
    pattern = _path_pattern(category, congress_start, congress_end)
    # List payloads contain the canonical Congress URL (including the
    # format=json query parameter), and raw_responses.endpoint_url has a unique
    # btree index. The previous implementation stripped query strings and
    # regex-scanned every stored detail response, which made a small historical
    # repair consume several minutes of CPU on the multi-million-row corpus.
    # Use the indexed exact-URL anti-join. Do not constrain the list URL itself:
    # older Congress.gov crawls often use the global /bill or /amendment list
    # route, while each item URL still carries the Congress number needed for
    # this scoped anti-join.
    query = """
        with source_failures as (
            select endpoint_url, count(*) as attempts
            from failed_requests
            where category = %s
              and node_type = %s
              and http_status in (404, 500)
            group by endpoint_url
        ), candidates as (
            select distinct on (regexp_replace(item->>'url', '[?].*$', ''))
                lists.id::text as parent_raw_id,
                lists.endpoint_url as parent_endpoint_url,
                item->>'url' as endpoint_url,
                regexp_replace(item->>'url', '[?].*$', '') as endpoint_path
            from raw_responses as lists
            cross join lateral jsonb_array_elements(
                coalesce(lists.response->%s, '[]'::jsonb)
            ) as item
            where lists.source = 'congress'
              and lists.category = %s
              and lists.node_type = %s
              and item->>'url' is not null
              and item->>'url' ~ %s
            order by regexp_replace(item->>'url', '[?].*$', ''), lists.fetched_at desc
        )
        select candidates.parent_raw_id, candidates.parent_endpoint_url, candidates.endpoint_path
        from candidates
        left join source_failures
          on source_failures.endpoint_url = candidates.endpoint_url
        where not exists (
            select 1
            from raw_responses as details
            where details.endpoint_url = candidates.endpoint_url
        )
          and (
              source_failures.attempts is null
              or source_failures.attempts < %s
          )
        order by candidates.endpoint_path
        limit %s
    """
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(
            query,
            (
                category,
                detail_node,
                item_key,
                category,
                list_node,
                pattern,
                max(1, max_source_error_attempts),
                max(1, limit),
            ),
        ).fetchall()
    return [
        RootDetail(
            endpoint_url=canonicalize_congress_api_url(row[2]),
            parent_raw_id=str(row[0]),
            parent_endpoint_url=str(row[1]),
        )
        for row in rows
    ]


async def run(args: argparse.Namespace) -> int:
    if args.congress_start < 1 or args.congress_end < args.congress_start:
        raise ValueError("Congress range must satisfy 1 <= start <= end")
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    details = load_missing(
        database_url=settings.database_url,
        category=args.category,
        congress_start=args.congress_start,
        congress_end=args.congress_end,
        limit=args.limit,
        max_source_error_attempts=args.max_source_error_attempts,
    )
    print(f"[root-detail] candidates={len(details)} category={args.category}", flush=True)
    if not details:
        return 0

    spec = load_spec(project_root / "specs" / f"{args.category}.yaml")
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=max(0.0, args.request_interval),
        active_key_budget=max(1, args.active_key_budget),
    )
    store = AsyncPostgresStore(settings.database_url)
    run_id = await store.start_run(args.category, "targeted_root_detail_backfill")
    semaphore = asyncio.Semaphore(max(1, args.concurrency))
    succeeded = 0
    failed = 0
    counter_lock = asyncio.Lock()

    async def fetch(detail: RootDetail) -> None:
        nonlocal succeeded, failed
        started_at = time.perf_counter()
        metadata_values: dict[str, Any] = {}
        try:
            async with semaphore:
                result = await client.get_json(
                    detail.endpoint_url,
                    endpoint_name=f"congress-{args.category}-root-detail",
                    endpoint_priority=4,
                )
            metadata_values = fetch_metadata_values(result.metadata)
            response_ref = await store.upsert_response(
                endpoint_url=detail.endpoint_url,
                source="congress",
                category=args.category,
                node_type=ROOTS[args.category][2],
                parent_id=detail.parent_raw_id,
                response=result.payload,
                latest_crawl_run_id=run_id,
            )
            await store.record_fetch(
                crawl_run_id=run_id,
                endpoint_url=detail.endpoint_url,
                raw_response_id=response_ref.raw_response_id,
                parent_endpoint_url=detail.parent_endpoint_url,
                node_type=ROOTS[args.category][2],
                follow_mode="targeted",
                http_status=metadata_values["status_code"],
                outcome="fetched",
                raw_response_version_id=response_ref.raw_response_version_id,
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
                category=args.category,
                node_type=ROOTS[args.category][2],
                endpoint_url=detail.endpoint_url,
                parent_endpoint_url=detail.parent_endpoint_url,
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
                endpoint_url=detail.endpoint_url,
                raw_response_id=None,
                parent_endpoint_url=detail.parent_endpoint_url,
                node_type=ROOTS[args.category][2],
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
        for batch_start in range(0, len(details), batch_size):
            batch = details[batch_start : batch_start + batch_size]
            await asyncio.gather(*(fetch(detail) for detail in batch))
        await store.finish_run(run_id, "completed", succeeded)
    except BaseException:
        await store.finish_run(run_id, "failed", succeeded)
        raise
    finally:
        await store.close()
    print(f"[root-detail] completed run={run_id} fetched={succeeded} failed={failed}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    parsed_args = parse_args()
    if (
        parsed_args.limit < 1
        or parsed_args.concurrency < 1
        or parsed_args.batch_size < 1
        or parsed_args.active_key_budget < 1
        or parsed_args.max_source_error_attempts < 1
    ):
        raise SystemExit(
            "limit, concurrency, batch-size, active-key-budget, and "
            "max-source-error-attempts must be positive"
        )
    raise SystemExit(asyncio.run(run(parsed_args)))
