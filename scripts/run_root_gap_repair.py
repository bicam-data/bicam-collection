"""Fetch missing historical Congress list pages without fanning out children.

The main historical workers deliberately walk stored descendants.  This small
repair pass closes known root-list page gaps in parallel with that work, while
leaving the stored payloads available for the normal descendant backfill later.
It is intentionally bounded by the current pagination-coverage snapshot and
uses ``backfill_descendants`` so a page fetched by another worker is skipped.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import psycopg

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import load_spec, resolve_spec_path
from bicam_collection.storage import AsyncPostgresStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--categories", nargs="+", default=["bills", "amendments"])
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument("--request-interval", type=float, default=5.0)
    parser.add_argument("--key-wait-timeout", type=float, default=600.0)
    parser.add_argument("--active-key-budget", type=int, default=17)
    parser.add_argument("--max-pages-per-partition", type=int, default=1000)
    return parser.parse_args()


def load_gaps(database_url: str, categories: list[str]) -> list[tuple[str, str, int, int]]:
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(
            """
            select coverage.category, coverage.partition_value,
                   lower(gap)::bigint as gap_start,
                   upper(gap)::bigint as gap_end
            from fct_pagination_coverage as coverage
            cross join lateral unnest(coverage.actionable_missing_position_ranges) as gap
            where coverage.category = any(%s)
              and coverage.actionable_missing_item_count > 0
            order by coverage.category, coverage.partition_value, lower(gap)
            """,
            (categories,),
        ).fetchall()
    return [
        (
            str(category),
            str(partition_value),
            int(gap_start),
            int(gap_end),
        )
        for category, partition_value, gap_start, gap_end in rows
    ]


def root_only_spec(spec):
    # Work on the loaded in-memory spec only.  The normal crawler specs retain
    # all child edges, so subsequent descendant backfills are unaffected.
    for node in spec.nodes.values():
        node.child_edges = []
        node.linked_item_edges = []
    return spec


def count_run_failures(database_url: str, run_id: str) -> int:
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            "select count(*) from crawl_run_fetches where crawl_run_id = %s and outcome = 'failed'",
            (run_id,),
        ).fetchone()
    return int(row[0] if row else 0)


async def run(args: argparse.Namespace) -> int:
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    gaps = load_gaps(settings.database_url, args.categories)
    if not gaps:
        print("no root pagination gaps found", flush=True)
        return 0

    specs = {
        category: root_only_spec(load_spec(resolve_spec_path(project_root / "specs", category)))
        for category in args.categories
    }
    client = build_client(
        spec=next(iter(specs.values())),
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=args.request_interval,
        active_key_budget=args.active_key_budget,
    )
    store = AsyncPostgresStore(settings.database_url)
    processed = failed = 0
    try:
        for category, partition_value, gap_start, gap_end in gaps:
            spec = specs[category]
            # ``*`` is an unpartitioned root such as the congressional-record
            # index; partitioned families append their Congress number.
            root_path = spec.seed.path.rstrip("/")
            if partition_value != "*":
                root_path += f"/{partition_value}"
            offsets = range(gap_start, gap_end, args.page_size)
            for page_number, offset in enumerate(offsets):
                if page_number >= args.max_pages_per_partition:
                    break
                seed_path = f"{root_path}?offset={offset}"
                crawler = AsyncSpecCrawler(
                    base_url=base_url_for_spec(settings, spec),
                    spec=spec,
                    client=client,
                    store=store,
                    concurrency=1,
                )
                try:
                    run_id = await crawler.run(
                        CrawlOptions(
                            mode="backfill_descendants",
                            seed_path=seed_path,
                            max_pages=1,
                            page_size=args.page_size,
                        )
                    )
                    run_failures = count_run_failures(settings.database_url, run_id)
                    if run_failures:
                        failed += run_failures
                        print(
                            f"[root-gap] page failed category={category} partition={partition_value} "
                            f"offset={offset} failures={run_failures}",
                            flush=True,
                        )
                    else:
                        processed += 1
                except Exception as exc:
                    failed += 1
                    print(f"[root-gap] failed category={category} partition={partition_value} offset={offset}: {exc}", flush=True)
                    continue
                if processed % 25 == 0 and processed:
                    print(
                        f"[root-gap] progress processed={processed} failed={failed} "
                        f"last={category}:{partition_value}@{offset}",
                        flush=True,
                    )
    finally:
        await store.close()
    print(f"[root-gap] completed processed={processed} failed={failed}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run(parse_args())))
