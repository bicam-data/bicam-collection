"""Harvest every page of the legacy Congressional Record index endpoint.

Unlike the other Congress.gov list responses, ``/congressional-record`` uses
``Results.TotalCount``/``Results.SetSize`` and does not return a ``next`` URL.
This runner plans explicit offsets and lets the normal declarative crawler
store each page with the same lineage and retry behavior as other categories.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import AsyncPostgresStore


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["full", "refresh", "backfill_descendants"], default="full")
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--request-interval", type=float, default=0.5)
    parser.add_argument("--key-wait-timeout", type=float, default=1200.0)
    parser.add_argument("--active-key-budget", type=int, default=4)
    return parser.parse_args(argv)


def page_offsets(total_count: int, page_size: int) -> list[int]:
    if total_count < 0 or page_size < 1:
        raise ValueError("total count must be non-negative and page size positive")
    return list(range(0, total_count, page_size)) or [0]


async def run(args: argparse.Namespace) -> int:
    if min(args.page_size, args.workers, args.concurrency, args.active_key_budget) < 1:
        raise ValueError("page size, workers, concurrency, and key budget must be positive")
    if args.request_interval < 0:
        raise ValueError("request interval must be non-negative")

    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    spec = load_spec(project_root / "specs" / "congressional-record.yaml")
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=args.request_interval,
        active_key_budget=args.active_key_budget,
    )
    store = AsyncPostgresStore(settings.database_url)
    probe = await client.get_json(
        f"{base_url_for_spec(settings, spec).rstrip('/')}/congressional-record"
        f"?format=json&offset=0&limit={args.page_size}",
        endpoint_name="congress-congressional-record-index-probe",
        endpoint_priority=3,
    )
    total_count = int(probe.payload.get("Results", {}).get("TotalCount", 0))
    offsets = asyncio.Queue[int]()
    for offset in page_offsets(total_count, args.page_size):
        offsets.put_nowait(offset)
    print(
        f"[record-index] total={total_count} pages={offsets.qsize()} page_size={args.page_size}",
        flush=True,
    )
    failures = 0
    completed = 0
    counter_lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal failures, completed
        while True:
            try:
                offset = offsets.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                crawler = AsyncSpecCrawler(
                    base_url=base_url_for_spec(settings, spec),
                    spec=spec,
                    client=client,
                    store=store,
                    concurrency=args.concurrency,
                )
                await crawler.run(
                    CrawlOptions(
                        mode=args.mode,
                        seed_path=f"/congressional-record?offset={offset}",
                        seed_node_type="congressional_record_list",
                        max_pages=1,
                        page_size=args.page_size,
                    )
                )
                async with counter_lock:
                    completed += 1
            except Exception as exc:
                async with counter_lock:
                    failures += 1
                print(f"[record-index] offset={offset} failed: {exc}", flush=True)
            finally:
                offsets.task_done()

    try:
        await asyncio.gather(*(worker() for _ in range(min(args.workers, max(1, offsets.qsize())))))
    finally:
        await store.close()
    print(f"[record-index] completed={completed} failed={failures}", flush=True)
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
