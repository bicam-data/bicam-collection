"""Repair nomination nominee groups whose stored pages are short of source counts.

Congress.gov normally exposes nominee-group pages with a useful ``next`` link,
but a small number of historical groups return an empty page when the crawler
asks for ``limit=250``.  The broad descendant pass quite correctly records the
response, yet cannot discover the remaining pages.  This bounded repair lane
finds only those groups and resumes at the observed offset with the API's
20-row page size, avoiding duplicate first-page observations.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
from pathlib import Path

import psycopg

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions, QueueItem
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import load_spec, resolve_spec_path
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.urls import replace_query_params


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--page-size", type=int, default=20)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--request-interval", type=float, default=0.2)
    parser.add_argument("--key-wait-timeout", type=float, default=1200.0)
    parser.add_argument("--active-key-budget", type=int, default=12)
    parser.add_argument("--max-groups", type=int, default=0, help="limit groups (0 means all gaps)")
    return parser.parse_args(argv)


def _find_gaps(database_url: str, max_groups: int = 0) -> list[tuple[str, int, int]]:
    """Return (endpoint path, expected count, observed count) for short groups."""
    limit_clause = "" if max_groups <= 0 else "limit %s"
    params: tuple[object, ...] = () if max_groups <= 0 else (max_groups,)
    query = f"""
        with expected as (
            select
                regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path,
                max((response -> 'pagination' ->> 'count')::bigint) as expected_items
            from raw_responses
            where source = 'congress'
              and category = 'nominations'
              and node_type = 'nomination_nominee_detail'
              and response -> 'pagination' ->> 'count' is not null
            group by 1
        ),
        fetched as (
            select
                regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path,
                count(*) as fetched_items
            from raw_responses
            cross join lateral jsonb_array_elements(
                coalesce(response -> 'nominees', '[]'::jsonb)
            ) as items(item)
            where source = 'congress'
              and category = 'nominations'
              and node_type = 'nomination_nominee_detail'
            group by 1
        )
        select expected.endpoint_path,
               expected.expected_items::bigint,
               coalesce(fetched.fetched_items, 0)::bigint
        from expected
        left join fetched using (endpoint_path)
        where expected.expected_items > coalesce(fetched.fetched_items, 0)
        order by expected.endpoint_path
        {limit_clause}
    """
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(query, params).fetchall()
    return [(str(path), int(expected), int(observed)) for path, expected, observed in rows]


async def _run(args: argparse.Namespace, project_root: Path) -> int:
    if args.concurrency < 1 or args.page_size < 1 or args.max_pages < 1:
        raise ValueError("concurrency, page-size, and max-pages must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0 or args.active_key_budget < 1:
        raise ValueError("request-interval must be non-negative; key-wait-timeout and active-key-budget must be positive")

    settings = Settings.from_env(project_root)
    gaps = _find_gaps(settings.database_url, args.max_groups)
    print(f"[nominee-gap-repair] groups={len(gaps)}", flush=True)
    if not gaps:
        return 0

    spec = load_spec(resolve_spec_path(project_root / "specs", "nominations"))
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=args.request_interval,
        active_key_budget=args.active_key_budget,
    )
    store = AsyncPostgresStore(settings.database_url)
    crawler = AsyncSpecCrawler(
        base_url=base_url_for_spec(settings, spec),
        spec=spec,
        client=client,
        store=store,
        concurrency=args.concurrency,
    )
    queue: deque[QueueItem] = deque()
    for endpoint_path, _expected, observed in gaps:
        # Existing responses contain the first ``observed`` rows. Start at
        # that offset so the repair does not duplicate the first page.
        offset = observed if observed > 0 else None
        url = replace_query_params(
            endpoint_path,
            {"offset": offset, "limit": args.page_size, "format": "json"},
        )
        queue.append(
            QueueItem(
                url=url,
                node_type="nomination_nominee_detail",
                follow_mode="recurse",
                parent_endpoint_url=endpoint_path,
                pagination_key=endpoint_path,
            )
        )

    run_id = await store.start_run("nominations", "repair_nominee_pagination")
    try:
        fetched, _ = await crawler._drain_queue(  # noqa: SLF001 - bounded repair uses crawler's durable queue engine
            run_id=run_id,
            queue=queue,
            options=CrawlOptions(
                mode="backfill_refresh_descendants",
                max_pages=args.max_pages,
                page_size=args.page_size,
            ),
            high_water_mark=None,
        )
        await store.finish_run(run_id, "completed", fetched)
        print(f"[nominee-gap-repair] completed pages={fetched} run={run_id}", flush=True)
        return 0
    except Exception:
        await store.finish_run(run_id, "failed", 0)
        raise
    finally:
        await store.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(_run(args, Path(__file__).resolve().parents[1]))


if __name__ == "__main__":
    raise SystemExit(main())
