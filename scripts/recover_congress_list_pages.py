"""Recover Congress.gov list pages whose large page size returns HTTP 500.

Some historical Congress.gov list pages fail when a page contains one or more
bad source records.  A smaller page can still succeed, so this helper divides
the requested offset range recursively and stores every successful sub-page
through the normal immutable raw-response path.  Irreducible single-record
failures remain in ``failed_requests`` as auditable source errors.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
from pathlib import Path

import psycopg

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import AsyncPostgresStore


ROOTS = {
    "bills": ("bill", "bill_list"),
    "amendments": ("amendment", "amendment_list"),
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--category", choices=sorted(ROOTS), required=True)
    parser.add_argument("--congress-start", type=int, required=True)
    parser.add_argument("--congress-end", type=int, required=True)
    parser.add_argument("--start-offset", type=int, default=0)
    parser.add_argument("--end-offset", type=int, required=True)
    parser.add_argument("--chunk-size", type=int, default=10)
    parser.add_argument("--min-chunk-size", type=int, default=1)
    parser.add_argument("--request-interval", type=float, default=0.5)
    parser.add_argument("--key-wait-timeout", type=float, default=600.0)
    parser.add_argument("--active-key-budget", type=int, default=1)
    parser.add_argument("--max-attempts", type=int, default=5000)
    parser.add_argument(
        "--terminal-failure-attempts",
        type=int,
        default=3,
        help="Do not retry an exact chunk after this many recorded HTTP 500 failures.",
    )
    return parser.parse_args(argv)


def _validate_args(args: argparse.Namespace) -> None:
    if args.congress_start < 1 or args.congress_end < args.congress_start:
        raise ValueError("Congress range must satisfy 1 <= start <= end")
    if args.start_offset < 0 or args.end_offset <= args.start_offset:
        raise ValueError("offset range must satisfy 0 <= start < end")
    if args.chunk_size < 1 or args.min_chunk_size < 1 or args.min_chunk_size > args.chunk_size:
        raise ValueError("chunk sizes must satisfy 1 <= min <= chunk")
    if args.request_interval < 0 or args.active_key_budget < 1:
        raise ValueError("request interval must be non-negative and key budget positive")
    if args.max_attempts < 1 or args.terminal_failure_attempts < 1:
        raise ValueError("attempt limits must be positive")


def _terminal_chunks(
    *, database_url: str, category: str, attempts: int
) -> set[str]:
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(
            """
            select endpoint_url
            from failed_requests
            where category = %s
              and node_type = %s
              and http_status = 500
            group by endpoint_url
            having count(*) >= %s
            """,
            (category, ROOTS[category][1], attempts),
        ).fetchall()
    return {str(row[0]) for row in rows}


async def run(args: argparse.Namespace, *, project_root: Path) -> tuple[int, int, int]:
    _validate_args(args)
    settings = Settings.from_env(project_root)
    spec = load_spec(project_root / "specs" / f"{args.category}.yaml")
    retry_spec = copy.deepcopy(spec)
    for node in retry_spec.nodes.values():
        node.child_edges = []
        node.linked_item_edges = []
    client = build_client(
        spec=retry_spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=args.request_interval,
        active_key_budget=args.active_key_budget,
    )
    store = AsyncPostgresStore(settings.database_url)
    terminal_paths = _terminal_chunks(
        database_url=settings.database_url,
        category=args.category,
        attempts=args.terminal_failure_attempts,
    )
    resource, node_type = ROOTS[args.category]
    attempted = 0
    successful = 0
    terminal = 0

    async def recover(congress: int, offset: int, size: int) -> None:
        nonlocal attempted, successful, terminal
        if attempted >= args.max_attempts:
            return
        endpoint_path = f"https://api.congress.gov/v3/{resource}/{congress}"
        endpoint_url = f"{endpoint_path}?format=json&limit={size}&offset={offset}"
        if await store.has_response(endpoint_url):
            successful += 1
            return
        if endpoint_url in terminal_paths and size == args.min_chunk_size:
            terminal += 1
            return
        attempted += 1
        crawler = AsyncSpecCrawler(
            base_url=base_url_for_spec(settings, retry_spec),
            spec=retry_spec,
            client=client,
            store=store,
            concurrency=1,
        )
        await crawler.run(
            CrawlOptions(
                mode="full",
                seed_path=f"/{resource}/{congress}?format=json&offset={offset}",
                seed_node_type=node_type,
                max_pages=1,
                page_size=size,
            )
        )
        if await store.has_response(endpoint_url):
            successful += 1
            return
        if size <= args.min_chunk_size:
            terminal += 1
            terminal_paths.add(endpoint_url)
            return
        left = size // 2
        await recover(congress, offset, left)
        await recover(congress, offset + left, size - left)

    try:
        for congress in range(args.congress_start, args.congress_end + 1):
            for offset in range(args.start_offset, args.end_offset, args.chunk_size):
                if attempted >= args.max_attempts:
                    break
                await recover(congress, offset, min(args.chunk_size, args.end_offset - offset))
    finally:
        await store.close()
    print(
        f"[list-page-repair] category={args.category} attempted={attempted} "
        f"successful={successful} terminal_source_errors={terminal}",
        flush=True,
    )
    return attempted, successful, terminal


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]
    attempted, _successful, _terminal = asyncio.run(run(args, project_root=project_root))
    if attempted >= args.max_attempts:
        return 2
    # Irreducible single-record 500s are durable source evidence, not a
    # worker/runtime failure.  Keep them in the report and failure ledger,
    # but let the bounded repair service complete successfully so systemd
    # does not treat an expected upstream defect as a crashed worker.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
