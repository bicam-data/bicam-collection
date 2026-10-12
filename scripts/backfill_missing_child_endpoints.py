"""Fetch only Congress child endpoints that the coverage audit proves missing.

The normal descendant refresh walks every stored bill detail root.  Once its
checkpoint is complete, newly discovered child URLs can otherwise wait for a
full replay cycle.  This bounded repair reads the same declared bill edges as
``check_child_coverage.py``, excludes confirmed source-side 404/500 gaps, and
feeds the remaining URLs through the normal crawler persistence/telemetry
path.  It is intentionally limited to bill descendants and a finite item
budget so it is safe to run alongside the scheduled refresh lane.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
from pathlib import Path

import psycopg
import yaml

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions, QueueItem
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import load_spec, resolve_spec_path
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.target_scope import TARGET_CONGRESS_CATEGORIES


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-items", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--request-interval", type=float, default=0.25)
    parser.add_argument("--key-wait-timeout", type=float, default=3600.0)
    parser.add_argument("--active-key-budget", type=int, default=17)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def _edges(project_root: Path) -> list[tuple[str, str, str, str, str | None, str | None, str]]:
    """Load declared Congress child edges without importing a script module."""
    edges: list[tuple[str, str, str, str, str | None, str | None]] = []
    for path in sorted((project_root / "specs").glob("*.yaml")):
        spec = yaml.safe_load(path.read_text(encoding="utf-8"))
        if spec.get("source") != "congress":
            continue
        category = str(spec.get("category"))
        if category not in TARGET_CONGRESS_CATEGORIES:
            continue
        for parent_node, node in (spec.get("nodes") or {}).items():
            if parent_node.endswith("_list"):
                continue
            for edge in node.get("child_edges") or []:
                edges.append(
                    (
                        category,
                        str(parent_node),
                        str(edge["target"]),
                        str(edge["url_path"]),
                        edge.get("fallback_url_suffix"),
                        edge.get("url_normalizer"),
                        str(edge.get("follow_mode", "recurse")),
                    )
                )
    return edges


def _missing_bill_children(database_url: str, project_root: Path, limit: int) -> list[dict[str, str]]:
    """Return one queue row per missing bill child endpoint.

    Endpoint paths are compared without query strings, matching the coverage
    audit.  The original advertised URL is retained so the crawler can apply
    its normal canonicalization and pagination parameters.
    """

    edges = [edge for edge in _edges(project_root) if edge[0] == "bills"]
    rows: list[dict[str, str]] = []
    with psycopg.connect(database_url) as connection:
        connection.execute("set statement_timeout='120s'")
        connection.execute("set lock_timeout='15s'")
        connection.execute("set max_parallel_workers_per_gather=0")
        connection.execute(
            """
            create temporary table child_paths as
            select distinct regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
            from raw_responses
            where source = 'congress'
            """
        )
        connection.execute("create index child_paths_idx on child_paths(endpoint_path)")
        connection.execute(
            """
            create temporary table source_errors as
            select regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
            from failed_requests where http_status = 404 group by 1
            union
            select regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
            from failed_requests where http_status = 500 group by 1 having count(*) >= 3
            """
        )
        connection.execute("create index source_errors_idx on source_errors(endpoint_path)")
        connection.execute("analyze child_paths")
        connection.execute("analyze source_errors")

        for category, parent_node, target_node, url_path, fallback_suffix, normalizer, follow_mode in edges:
            # Bill detail child edges are advertised as absolute URLs.  Keep
            # the fallback branch for completeness and to mirror the audit.
            query = """
                with parents as (
                    select id, endpoint_url, response
                    from raw_responses
                    where source = 'congress' and category = %s and node_type = %s
                ),
                advertised as (
                    select distinct
                        parent.id::text as parent_id,
                        parent.endpoint_url as parent_endpoint_url,
                        value #>> '{}' as advertised_url,
                        regexp_replace(value #>> '{}', '[?].*$', '') as endpoint_path
                    from parents as parent
                    cross join lateral jsonb_path_query(parent.response, %s::jsonpath) as value
                    where jsonb_typeof(value) = 'string' and value #>> '{}' like 'http%%'
                ),
                fallback as (
                    select
                        parent.id::text as parent_id,
                        parent.endpoint_url as parent_endpoint_url,
                        regexp_replace(parent.endpoint_url, '[?].*$', '') || '/' || %s as advertised_url,
                        regexp_replace(regexp_replace(parent.endpoint_url, '[?].*$', '') || '/' || %s, '[?].*$', '') as endpoint_path
                    from parents as parent
                    where %s::text is not null
                      and not exists (select 1 from advertised where advertised.parent_id = parent.id::text)
                ),
                expected as (
                    select * from advertised
                    union all
                    select * from fallback
                )
                select distinct on (endpoint_path)
                    parent_id, parent_endpoint_url, advertised_url
                from expected
                left join child_paths using (endpoint_path)
                left join source_errors using (endpoint_path)
                where child_paths.endpoint_path is null
                  and source_errors.endpoint_path is null
                order by endpoint_path, parent_id
            """
            normalized_fallback = fallback_suffix.strip("/") if fallback_suffix else None
            result = connection.execute(
                query,
                (category, parent_node, url_path, normalized_fallback, normalized_fallback, normalized_fallback),
            )
            for parent_id, parent_endpoint_url, advertised_url in result:
                rows.append(
                    {
                        "parent_raw_id": str(parent_id),
                        "parent_endpoint_url": str(parent_endpoint_url),
                        "url": str(advertised_url),
                        "node_type": target_node,
                        "follow_mode": follow_mode,
                    }
                )
                if len(rows) >= limit:
                    return rows
    return rows


async def run(args: argparse.Namespace, *, project_root: Path) -> int:
    if args.max_items < 1 or args.concurrency < 1 or args.page_size < 1 or args.max_pages < 1:
        raise ValueError("max-items, concurrency, page-size, and max-pages must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0 or args.active_key_budget < 1:
        raise ValueError("request-interval must be non-negative; key-wait-timeout and active-key-budget must be positive")

    settings = Settings.from_env(project_root)
    missing = _missing_bill_children(settings.database_url, project_root, args.max_items)
    print(f"[missing-child-backfill] candidates={len(missing)}", flush=True)
    for row in missing[:10]:
        print(f"[missing-child-backfill] candidate={row['node_type']} {row['url']}", flush=True)
    if args.dry_run or not missing:
        return 0

    spec = load_spec(resolve_spec_path(project_root / "specs", "bills"))
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
    queue = deque(
        QueueItem(
            url=row["url"],
            node_type=row["node_type"],
            follow_mode=row["follow_mode"],
            parent_raw_id=row["parent_raw_id"],
            parent_endpoint_url=row["parent_endpoint_url"],
        )
        for row in missing
    )
    run_id = await store.start_run("bills", "backfill_missing_child_endpoints")
    try:
        fetched, _ = await crawler._drain_queue(
            run_id=run_id,
            queue=queue,
            options=CrawlOptions(
                mode="backfill_refresh_descendants",
                page_size=args.page_size,
                max_pages=args.max_pages,
            ),
            high_water_mark=None,
        )
        await store.finish_run(run_id, "completed", fetched)
        print(f"[missing-child-backfill] run={run_id} fetched={fetched}", flush=True)
        return 0
    except Exception:
        await store.finish_run(run_id, "failed", 0)
        raise
    finally:
        await store.close()


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(parse_args(argv), project_root=Path(__file__).resolve().parents[1]))


if __name__ == "__main__":
    raise SystemExit(main())
