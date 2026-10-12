"""Crawl Congress.gov per Congress via the ``/<resource>/<congress>`` routes (1-current).

The global list endpoints aren't partitioned by Congress, so they can't track
historical coverage.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import discover_spec_categories, load_spec, resolve_spec_path
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.target_scope import TARGET_CONGRESS_CATEGORIES


DEFAULT_CATEGORIES = TARGET_CONGRESS_CATEGORIES
UNPARTITIONED_CATEGORIES = {
    "bound-congressional-record",
    "congresses",
    "congressional-record",
    "crs-reports",
    "daily-congressional-record",
    "house-requirements",
    "members",
}

# Count fetch failures and keep going; the error log separates retryable URLs
# from partitions that need attention.
_RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429, 502, 503, 504, 520, 522, 523, 524})


def _classify_partition_exception(exc: BaseException) -> str:
    """Return a durable class for a failed historical partition."""

    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return "retryable"
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status is None:
        metadata = getattr(exc, "fetch_metadata", None)
        status = getattr(metadata, "status_code", None)
    if status in _RETRYABLE_HTTP_STATUSES:
        return "retryable"
    if status == 500 or "source_error=" in str(exc):
        return "source_error"
    text = str(exc).lower()
    if any(marker in text for marker in ("timed out", "temporarily unavailable", "connection reset")):
        return "retryable"
    # Configuration, spec and database errors stop the runner.
    return "fatal"


@dataclass(frozen=True, slots=True)
class Job:
    category: str
    congress: int | None


def parse_args(argv: list[str] | None = None, *, project_root: Path | None = None) -> argparse.Namespace:
    root = project_root or Path(__file__).resolve().parents[1]
    choices = discover_spec_categories(root / "specs")
    parser = argparse.ArgumentParser(description="Crawl Congress.gov by historical Congress partition")
    parser.add_argument("--categories", nargs="*", choices=choices, default=list(DEFAULT_CATEGORIES))
    parser.add_argument("--congress-start", type=int, default=1)
    parser.add_argument("--congress-end", type=int, default=119)
    # Not resume: its high-water mark would skip old Congresses.
    parser.add_argument(
        "--mode",
        choices=["full", "refresh", "resume", "backfill_descendants"],
        default="full",
        help=(
            "Use refresh to refetch the bounded root scope, or backfill_descendants "
            "to walk stored pagination/child links without refetching existing URLs."
        ),
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=3,
        help="Maximum pages per paginated node, including child lists (bounded by default).",
    )
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument(
        "--seed-offset",
        type=int,
        default=0,
        help="Starting offset for the partition's seed list; repeat with successive offsets for complete page-batched coverage.",
    )
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--category-workers", type=int, default=1)
    parser.add_argument(
        "--key-wait-timeout",
        type=float,
        default=120.0,
        help="Maximum seconds to wait for a rate-limited API key before failing a partition.",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=0.25,
        help="Minimum seconds between requests from this client; keep nonzero for historical backfills.",
    )
    parser.add_argument(
        "--active-key-budget",
        type=int,
        default=None,
        help="Maximum number of configured API keys used concurrently (defaults to the client's safety reserve).",
    )
    return parser.parse_args(argv)


def _partition_path(spec: object, congress: int | None, *, seed_offset: int = 0) -> str:
    seed_path = getattr(getattr(spec, "seed"), "path")
    if "{" in seed_path:
        raise ValueError(f"historical partitioning is not supported for templated seed path {seed_path!r}")
    path = seed_path.rstrip("/")
    if congress is not None and getattr(spec, "category", None) not in UNPARTITIONED_CATEGORIES:
        path = f"{path}/{congress}"
    if seed_offset:
        separator = "&" if "?" in path else "?"
        path = f"{path}{separator}offset={seed_offset}"
    return path


def _ordered_jobs(categories: list[str], congress_start: int, congress_end: int) -> list[Job]:
    """Return partition jobs interleaved by Congress, then unpartitioned jobs."""
    partitioned_categories = [
        category for category in categories if category not in UNPARTITIONED_CATEGORIES
    ]
    jobs = [
        Job(category=category, congress=congress)
        for congress in range(congress_start, congress_end + 1)
        for category in partitioned_categories
    ]
    jobs.extend(Job(category=category, congress=None) for category in categories if category in UNPARTITIONED_CATEGORIES)
    return jobs


async def run_history(args: argparse.Namespace, *, project_root: Path) -> int:
    if args.congress_start < 1 or args.congress_end < args.congress_start:
        raise ValueError("Congress range must satisfy 1 <= congress-start <= congress-end")
    if args.seed_offset < 0:
        raise ValueError("seed-offset must be non-negative")
    if args.request_interval < 0:
        raise ValueError("request-interval must be non-negative")
    if args.active_key_budget is not None and args.active_key_budget < 1:
        raise ValueError("active-key-budget must be positive")

    settings = Settings.from_env(project_root)
    categories = list(args.categories)
    specs = {category: load_spec(resolve_spec_path(project_root / "specs", category)) for category in categories}
    non_congress = [spec.category for spec in specs.values() if spec.source != "congress"]
    if non_congress:
        raise ValueError(f"historical runner only accepts Congress.gov categories: {non_congress}")
    clients = {
        "congress": build_client(
            spec=next(iter(specs.values())),
            env_path=settings.env_path,
            use_async=True,
            key_wait_timeout=args.key_wait_timeout,
            request_interval=args.request_interval,
            active_key_budget=args.active_key_budget,
        )
    }
    store = AsyncPostgresStore(settings.database_url)
    jobs = asyncio.Queue[Job]()
    # Interleave categories by Congress so one category can't starve the others.
    for job in _ordered_jobs(categories, args.congress_start, args.congress_end):
        jobs.put_nowait(job)

    successes = 0
    failures = 0
    counter_lock = asyncio.Lock()
    worker_count = max(1, min(args.category_workers, jobs.qsize()))

    async def worker() -> None:
        nonlocal successes, failures
        while True:
            try:
                job = jobs.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                spec = specs[job.category]
                crawler = AsyncSpecCrawler(
                    base_url=base_url_for_spec(settings, spec),
                    spec=spec,
                    client=clients["congress"],
                    store=store,
                    concurrency=max(1, args.concurrency),
                )
                run_id = await crawler.run(
                    CrawlOptions(
                        mode=args.mode,
                        seed_path=_partition_path(spec, job.congress, seed_offset=args.seed_offset),
                        max_pages=args.max_pages,
                        page_size=args.page_size,
                    )
                )
                async with counter_lock:
                    successes += 1
                scope = f"congress={job.congress}" if job.congress is not None else "unpartitioned"
                print(f"[history] completed {job.category} {scope} run={run_id}", flush=True)
            except Exception as exc:  # keep oldest-first progress moving after one bad source partition
                async with counter_lock:
                    failures += 1
                scope = f"congress={job.congress}" if job.congress is not None else "unpartitioned"
                failure_class = _classify_partition_exception(exc)
                print(
                    f"[history] failed {job.category} {scope} "
                    f"class={failure_class}: {exc}",
                    flush=True,
                )
                if failure_class == "fatal":
                    raise
            finally:
                jobs.task_done()

    try:
        await asyncio.gather(*(worker() for _ in range(worker_count)))
    finally:
        await store.close()
    print(f"[history] completed={successes} failed={failures} jobs={successes + failures}")
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(run_history(args, project_root=Path(__file__).resolve().parents[1]))


if __name__ == "__main__":
    raise SystemExit(main())
