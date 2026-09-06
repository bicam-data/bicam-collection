"""Backfill missing child URLs from every stored detail record.

Some historical Congress.gov list partitions (especially amendments) return
empty responses even though their detail records were discovered through the
global list.  A partition crawl therefore cannot reach those details' child
URLs.  This runner walks stored refresh roots in stable endpoint-URL keyset batches and
uses ``backfill_refresh_descendants`` so existing child payloads are skipped
while missing descendants are fetched.  The ``--source`` switch also supports
GovInfo collections whose stored package/detail roots need a historical
descendant pass (for example Congressional Directories).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path

import psycopg

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import load_spec, resolve_spec_path
from bicam_collection.storage import AsyncPostgresStore


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=["congress", "govinfo"],
        default="congress",
        help="API source whose stored refresh roots should be traversed",
    )
    parser.add_argument("--categories", nargs="+", default=["amendments"])
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--request-interval", type=float, default=0.05)
    parser.add_argument("--key-wait-timeout", type=float, default=3600.0)
    parser.add_argument("--active-key-budget", type=int, default=32)
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="yield after this many batches across categories (0 means run all batches)",
    )
    parser.add_argument(
        "--checkpoint-path",
        type=Path,
        default=None,
        help="JSON cursor file used to resume completed root batches",
    )
    return parser.parse_args(argv)


def _load_checkpoint(path: Path | None) -> dict[str, object]:
    if path is None or not path.exists():
        return {"version": 2, "categories": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid descendant checkpoint {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"unsupported descendant checkpoint format: {path}")
    categories = payload.get("categories")
    if not isinstance(categories, dict):
        raise ValueError(f"descendant checkpoint categories must be an object: {path}")
    if payload.get("version") == 1:
        # OFFSET over mutable fetched_at could already have skipped roots.  A
        # v1 cursor cannot be converted safely, so reset each category and let
        # idempotent backfill traversal re-check it from the beginning.
        migrated_categories: dict[str, object] = {}
        for category, raw in categories.items():
            previous_root_count = 0
            if isinstance(raw, dict):
                previous_root_count = int(raw.get("root_count", 0) or 0)
            migrated_categories[str(category)] = {
                "after_endpoint_url": None,
                "cycle_start_root_count": previous_root_count,
                "observed_root_count": previous_root_count,
                "completed": False,
                "reset_from_version": 1,
            }
        return {"version": 2, "categories": migrated_categories}
    if payload.get("version") != 2:
        raise ValueError(f"unsupported descendant checkpoint format: {path}")
    return payload


def _save_checkpoint(path: Path | None, payload: dict[str, object]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _category_cursor(
    payload: dict[str, object],
    *,
    category: str,
    root_count: int,
    spec_fingerprint: str | None = None,
) -> dict[str, object]:
    categories = payload.setdefault("categories", {})
    assert isinstance(categories, dict)
    raw = categories.get(category)
    if not isinstance(raw, dict):
        raw = {}
    after_endpoint_url = raw.get("after_endpoint_url")
    if after_endpoint_url is not None and not isinstance(after_endpoint_url, str):
        raise ValueError(f"invalid endpoint cursor for category {category!r}")
    cycle_start_root_count = int(raw.get("cycle_start_root_count", root_count) or 0)
    completed = bool(raw.get("completed", False))
    stored_fingerprint = raw.get("spec_fingerprint")
    if spec_fingerprint is not None and stored_fingerprint != spec_fingerprint:
        # A newly declared child or pagination rule must replay stored roots
        # even when their population did not change. Existing responses are
        # still skipped by backfill_refresh_descendants, so this is bounded to
        # the newly reachable URLs/pages.
        after_endpoint_url = None
        cycle_start_root_count = root_count
        completed = False
    elif completed and root_count != cycle_start_root_count:
        # New roots can sort before an endpoint cursor.  Start a new complete
        # cycle whenever the population changes so no insertion is stranded.
        after_endpoint_url = None
        cycle_start_root_count = root_count
        completed = False
    elif not raw:
        cycle_start_root_count = root_count
    cursor = {
        "after_endpoint_url": after_endpoint_url,
        "cycle_start_root_count": cycle_start_root_count,
        "observed_root_count": root_count,
        "completed": completed,
    }
    if spec_fingerprint is not None:
        cursor["spec_fingerprint"] = spec_fingerprint
    categories[category] = cursor
    return cursor


def _root_count(database_url: str, *, source: str, category: str, node_type: str) -> int:
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """
            select count(*)
            from raw_responses
            where source = %s
              and category = %s
              and node_type = %s
            """,
            (source, category, node_type),
        ).fetchone()
    return int(row[0] if row else 0)


async def run(args: argparse.Namespace, *, project_root: Path) -> int:
    if args.batch_size < 1 or args.concurrency < 1 or args.page_size < 1 or args.max_pages < 1:
        raise ValueError("batch-size, concurrency, page-size, and max-pages must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0 or args.active_key_budget < 1:
        raise ValueError("request-interval must be non-negative; key-wait-timeout and active-key-budget must be positive")
    if args.max_batches < 0:
        raise ValueError("max-batches must be non-negative")

    settings = Settings.from_env(project_root)
    specs = {
        category: load_spec(resolve_spec_path(project_root / "specs", category))
        for category in args.categories
    }
    spec_fingerprints = {
        category: hashlib.sha256(
            resolve_spec_path(project_root / "specs", category).read_bytes()
        ).hexdigest()
        for category in args.categories
    }
    unsupported = [
        category
        for category, spec in specs.items()
        if spec.source != args.source or not spec.refresh_root_node_type
    ]
    if unsupported:
        raise ValueError(
            f"descendant refresh requires {args.source} specs with refresh roots: {unsupported}"
        )

    client = build_client(
        spec=next(iter(specs.values())),
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=args.request_interval,
        active_key_budget=args.active_key_budget,
    )
    store = AsyncPostgresStore(settings.database_url)
    failures = 0
    completed_batches = 0
    checkpoint = _load_checkpoint(args.checkpoint_path)
    try:
        # Initialize every category before doing network work, then process one
        # batch per category per round. A large category (nominations) must not
        # consume an entire bounded invocation while smaller item-level
        # backlogs (amendment actions/texts, bill actions) wait indefinitely.
        states: dict[str, tuple[object, dict[str, object], int]] = {}
        for category, spec in specs.items():
            node_type = spec.refresh_root_node_type
            assert node_type is not None
            root_count = _root_count(
                settings.database_url,
                source=args.source,
                category=category,
                node_type=node_type,
            )
            print(f"[descendant-refresh] category={category} roots={root_count}", flush=True)
            cursor = _category_cursor(
                checkpoint,
                category=category,
                root_count=root_count,
                spec_fingerprint=spec_fingerprints[category],
            )
            if root_count == 0:
                cursor["completed"] = True
            states[category] = (spec, cursor, root_count)
        _save_checkpoint(args.checkpoint_path, checkpoint)

        while True:
            round_progress = False
            pending_categories = False
            for category, (spec, cursor, root_count) in states.items():
                if bool(cursor["completed"]):
                    continue
                pending_categories = True
                batch_failed = False
                crawler = AsyncSpecCrawler(
                    base_url=base_url_for_spec(settings, spec),
                    spec=spec,
                    client=client,
                    store=store,
                    concurrency=args.concurrency,
                )
                try:
                    run_id = await crawler.run(
                        CrawlOptions(
                            mode="backfill_refresh_descendants",
                            batch_size=args.batch_size,
                            refresh_after_endpoint_url=cursor.get("after_endpoint_url"),
                            max_pages=args.max_pages,
                            page_size=args.page_size,
                        )
                    )
                    selected_roots = crawler.last_refresh_root_count
                    last_endpoint_url = crawler.last_refresh_endpoint_url
                    print(
                        f"[descendant-refresh] completed category={category} "
                        f"selected_roots={selected_roots} run={run_id}",
                        flush=True,
                    )
                except Exception as exc:
                    failures += 1
                    batch_failed = True
                    print(
                        f"[descendant-refresh] failed category={category} "
                        f"after_endpoint_url={cursor.get('after_endpoint_url')!r}: {exc}",
                        flush=True,
                    )
                else:
                    previous_endpoint_url = cursor.get("after_endpoint_url")
                    if selected_roots == 0:
                        cursor["completed"] = True
                    elif last_endpoint_url is None or last_endpoint_url == previous_endpoint_url:
                        raise RuntimeError(
                            f"descendant cursor did not advance for category {category!r}"
                        )
                    else:
                        cursor["after_endpoint_url"] = last_endpoint_url
                        cursor["completed"] = selected_roots < args.batch_size
                    cursor["observed_root_count"] = root_count
                    cursor.pop("reset_from_version", None)
                    _save_checkpoint(args.checkpoint_path, checkpoint)
                    round_progress = True
                completed_batches += 1
                if args.max_batches and completed_batches >= args.max_batches:
                    print(
                        f"[descendant-refresh] yielding after batches={completed_batches} "
                        f"category={category} "
                        f"after_endpoint_url={cursor.get('after_endpoint_url')!r}",
                        flush=True,
                    )
                    return 1 if failures else 0
                # A failed category gets another chance in a later round if a
                # different category made progress; avoid a tight retry loop
                # when every active source path is unavailable.
                if batch_failed:
                    continue
            if not pending_categories or not round_progress:
                break
    finally:
        await store.close()
    print(f"[descendant-refresh] completed failures={failures}", flush=True)
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(run(args, project_root=Path(__file__).resolve().parents[1]))


if __name__ == "__main__":
    raise SystemExit(main())
