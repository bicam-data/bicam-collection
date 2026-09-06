from __future__ import annotations

import argparse
import asyncio
import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import discover_spec_categories, load_spec, resolve_spec_path
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.target_scope import TARGET_CONGRESS_CATEGORIES


def _load_progress_reporter():
    run_crawl_path = Path(__file__).resolve().with_name("run_crawl.py")
    spec = importlib.util.spec_from_file_location("run_crawl", run_crawl_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {run_crawl_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.TqdmProgressReporter


TqdmProgressReporter = _load_progress_reporter()


def parse_args(argv: list[str] | None = None, *, project_root: Path | None = None) -> argparse.Namespace:
    resolved_project_root = project_root or Path(__file__).resolve().parents[1]
    available_categories = discover_spec_categories(resolved_project_root / "specs")
    parser = argparse.ArgumentParser(description="Run a full multi-category raw crawl in one process")
    default_categories = [
        category for category in TARGET_CONGRESS_CATEGORIES if category in available_categories
    ]
    parser.add_argument("--categories", nargs="*", choices=available_categories, default=default_categories)
    parser.add_argument("--mode", default="full", choices=["full", "incremental", "refresh", "resume", "refresh_descendants", "backfill_descendants"])
    parser.add_argument("--from-datetime")
    parser.add_argument("--to-datetime")
    parser.add_argument(
        "--overlap-hours",
        type=float,
        default=0.0,
        help=(
            "When an incremental/resume crawl uses the stored high-water mark, "
            "rewind it by this many hours to tolerate boundary ties and late source updates."
        ),
    )
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--page-size", type=int)
    parser.add_argument("--concurrency", type=int, default=5, help="Per-category in-flight fetch concurrency")
    parser.add_argument("--category-workers", type=int, default=2, help="Number of categories to crawl at once")
    parser.add_argument(
        "--key-wait-timeout",
        type=float,
        default=120.0,
        help="Maximum seconds to wait for a rate-limited API key before failing a category.",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=0.1,
        help="Minimum seconds between requests from each source client.",
    )
    parser.add_argument("--cleanup-stale-hours", type=float, default=1.0, help="Mark matching running rows older than this many hours as failed before starting")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


async def _run_category(
    *,
    spec,
    settings: Settings,
    client: object,
    store: AsyncPostgresStore,
    args: argparse.Namespace,
    semaphore: asyncio.Semaphore,
) -> tuple[str, str, float]:
    async with semaphore:
        from_datetime = args.from_datetime
        if (
            from_datetime is None
            and args.mode in {"incremental", "resume"}
            and args.overlap_hours > 0
        ):
            high_water_mark = await store.get_latest_high_water_mark(spec.category)
            if high_water_mark is not None:
                overlapped = high_water_mark - timedelta(hours=args.overlap_hours)
                from_datetime = overlapped.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        reporter = TqdmProgressReporter(verbose=args.verbose)
        crawler = AsyncSpecCrawler(
            base_url=base_url_for_spec(settings, spec),
            spec=spec,
            client=client,
            store=store,
            reporter=reporter,
            concurrency=args.concurrency,
        )
        started_at = time.perf_counter()
        run_id = await crawler.run(
            CrawlOptions(
                mode=args.mode,
                from_datetime=from_datetime,
                to_datetime=args.to_datetime,
                max_pages=args.max_pages,
                batch_size=args.batch_size,
                page_size=args.page_size,
            )
        )
        return spec.category, run_id, time.perf_counter() - started_at


async def _run_multi_crawl(args: argparse.Namespace, *, project_root: Path) -> int:
    if args.overlap_hours < 0:
        raise ValueError("overlap-hours must be non-negative")
    settings = Settings.from_env(project_root)
    store = AsyncPostgresStore(settings.database_url)
    semaphore = asyncio.Semaphore(max(1, min(args.category_workers, len(args.categories))))
    started_at = time.perf_counter()
    try:
        stale_before = datetime.now(timezone.utc) - timedelta(hours=max(0.0, args.cleanup_stale_hours))
        cleaned = await store.cleanup_stale_runs(categories=args.categories, stale_before=stale_before)
        if cleaned:
            print(f"[multi] marked {cleaned} stale running rows as failed")
        specs = {
            category: load_spec(resolve_spec_path(project_root / "specs", category))
            for category in args.categories
        }
        clients = {
            source: build_client(
                spec=next(spec for spec in specs.values() if spec.source == source),
                env_path=settings.env_path,
                use_async=True,
                key_wait_timeout=args.key_wait_timeout,
                request_interval=args.request_interval,
            )
            for source in sorted({spec.source for spec in specs.values()})
        }
        tasks = [
            asyncio.create_task(
                _run_category(
                    spec=specs[category],
                    settings=settings,
                    client=clients[specs[category].source],
                    store=store,
                    args=args,
                    semaphore=semaphore,
                )
            )
            for category in args.categories
        ]
        for task in asyncio.as_completed(tasks):
            category, run_id, elapsed = await task
            print(f"[multi] completed {category} run {run_id} in {elapsed:.1f}s")
        print(f"[multi] completed {len(args.categories)} categories in {time.perf_counter() - started_at:.1f}s")
        return 0
    finally:
        await store.close()


def main(argv: list[str] | None = None) -> int:
    project_root = Path(__file__).resolve().parents[1]
    args = parse_args(argv, project_root=project_root)
    return asyncio.run(_run_multi_crawl(args, project_root=project_root))


if __name__ == "__main__":
    raise SystemExit(main())
