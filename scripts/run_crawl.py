from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import time
from typing import Any

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions, SpecCrawler
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import discover_spec_categories, load_spec, resolve_spec_path
from bicam_collection.storage import AsyncPostgresStore, PostgresStore
from tqdm import tqdm


class TqdmProgressReporter:
    def __init__(self, *, category: str, verbose: bool = False, log_every: int = 100) -> None:
        self._category = category
        self._verbose = verbose
        self._log_every = log_every
        self._bar: tqdm[Any] | None = None
        self._started_at = time.perf_counter()
        self._total_duration_ms = 0
        self._total_attempts = 0
        self._total_rate_limit_attempts = 0
        self._failed_requests = 0

    def on_seed_progress(
        self,
        *,
        category: str,
        page_number: int,
        total_pages: int | None,
        items_seen: int,
        total_items: int | None,
    ) -> None:
        if self._bar is None:
            self._bar = tqdm(total=total_items, unit="items", dynamic_ncols=True)
        elif total_items is not None and self._bar.total != total_items:
            self._bar.total = total_items
        page_total = total_pages if total_pages is not None else "?"
        self._bar.set_description(f"{category} page {page_number}/{page_total}")
        delta = items_seen - self._bar.n
        if delta > 0:
            self._bar.update(delta)
        else:
            self._bar.refresh()

    def on_url_fetched(
        self,
        *,
        category: str,
        node_type: str,
        endpoint_url: str,
        http_status: int | None,
        total_fetched: int,
        duration_ms: int | None = None,
        attempt_count: int | None = None,
        rate_limit_attempts: int | None = None,
        api_key_name: str | None = None,
        pool_allocation: dict[str, Any] | None = None,
    ) -> None:
        if duration_ms is not None:
            self._total_duration_ms += duration_ms
        if attempt_count is not None:
            self._total_attempts += attempt_count
        if rate_limit_attempts is not None:
            self._total_rate_limit_attempts += rate_limit_attempts
        if self._verbose:
            details = []
            if duration_ms is not None:
                details.append(f"{duration_ms}ms")
            if attempt_count and attempt_count > 1:
                details.append(f"attempts={attempt_count}")
            if rate_limit_attempts:
                details.append(f"429s={rate_limit_attempts}")
            if api_key_name:
                details.append(f"key={api_key_name}")
            if pool_allocation:
                details.append(_format_pool_allocation(pool_allocation))
            suffix = f" ({', '.join(details)})" if details else ""
            tqdm.write(f"[{category}] fetched {total_fetched} [{node_type}] {http_status or '-'} {endpoint_url}{suffix}")
            return
        if total_fetched % self._log_every == 0:
            avg_ms = round(self._total_duration_ms / total_fetched) if total_fetched and self._total_duration_ms else None
            throughput = total_fetched / max(1e-6, time.perf_counter() - self._started_at)
            extras = []
            if avg_ms is not None:
                extras.append(f"avg={avg_ms}ms")
            extras.append(f"{throughput:.2f} url/s")
            if self._total_rate_limit_attempts:
                extras.append(f"429 retries={self._total_rate_limit_attempts}")
            tqdm.write(
                f"[{category}] fetched {total_fetched} URLs; last [{node_type}] {http_status or '-'} {endpoint_url} ({', '.join(extras)})"
            )

    def on_request_failed(
        self,
        *,
        category: str,
        node_type: str,
        endpoint_url: str,
        http_status: int | None,
        error: str,
        total_failed: int,
        duration_ms: int | None = None,
        attempt_count: int | None = None,
        rate_limit_attempts: int | None = None,
        api_key_name: str | None = None,
        pool_allocation: dict[str, Any] | None = None,
    ) -> None:
        self._failed_requests = total_failed
        if duration_ms is not None:
            self._total_duration_ms += duration_ms
        if attempt_count is not None:
            self._total_attempts += attempt_count
        if rate_limit_attempts is not None:
            self._total_rate_limit_attempts += rate_limit_attempts
        details = []
        if duration_ms is not None:
            details.append(f"{duration_ms}ms")
        if attempt_count:
            details.append(f"attempts={attempt_count}")
        if rate_limit_attempts:
            details.append(f"429s={rate_limit_attempts}")
        if api_key_name:
            details.append(f"key={api_key_name}")
        if pool_allocation:
            details.append(_format_pool_allocation(pool_allocation))
        suffix = f" ({', '.join(details)})" if details else ""
        tqdm.write(
            f"[{category}] failed {total_failed} [{node_type}] {http_status or '-'} {endpoint_url}: {error}{suffix}"
        )

    def on_run_finished(
        self,
        *,
        category: str,
        run_id: str,
        status: str,
        items_fetched: int,
    ) -> None:
        if self._bar is not None:
            self._bar.close()
        elapsed = max(1e-6, time.perf_counter() - self._started_at)
        avg_ms = round(self._total_duration_ms / items_fetched) if items_fetched and self._total_duration_ms else None
        avg_attempts = round(self._total_attempts / items_fetched, 2) if items_fetched and self._total_attempts else None
        extras = [f"{elapsed:.1f}s elapsed", f"{items_fetched / elapsed:.2f} url/s"]
        if avg_ms is not None:
            extras.append(f"avg={avg_ms}ms")
        if avg_attempts is not None:
            extras.append(f"avg attempts={avg_attempts}")
        if self._total_rate_limit_attempts:
            extras.append(f"429 retries={self._total_rate_limit_attempts}")
        if self._failed_requests:
            extras.append(f"failures={self._failed_requests}")
        tqdm.write(f"[{category}] {status} run {run_id} with {items_fetched} fetched URLs ({', '.join(extras)})")


def _format_pool_allocation(pool_allocation: dict[str, Any]) -> str:
    endpoint = pool_allocation.get("endpoint_name") or "?"
    priority = pool_allocation.get("endpoint_priority") or "?"
    reserve = pool_allocation.get("endpoint_reserve") or "?"
    selected_key = pool_allocation.get("selected_key_name") or "-"
    active_endpoints = pool_allocation.get("active_endpoints") or []
    active_summary = "/".join(
        f"{entry.get('name')}:{entry.get('assigned_key_count', 0)}"
        for entry in active_endpoints[:3]
    )
    if len(active_endpoints) > 3:
        active_summary = f"{active_summary}+{len(active_endpoints) - 3}"
    if not active_summary:
        active_summary = "-"
    return f"alloc={endpoint}:p{priority}:r{reserve}:key={selected_key}:active={active_summary}"


def parse_args(argv: list[str] | None = None, *, project_root: Path | None = None) -> argparse.Namespace:
    resolved_project_root = project_root or Path(__file__).resolve().parents[1]
    available_categories = discover_spec_categories(resolved_project_root / "specs")
    parser = argparse.ArgumentParser(description="Run a spec-driven raw crawl")
    parser.add_argument(
        "--category",
        required=True,
        help=f"Spec category from specs/*.yaml. Available: {', '.join(available_categories)}",
    )
    parser.add_argument("--mode", required=True, choices=["full", "incremental", "refresh", "resume", "refresh_descendants", "backfill_descendants"])
    parser.add_argument("--from-datetime")
    parser.add_argument("--to-datetime")
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--page-size", type=int)
    parser.add_argument("--async", dest="use_async", action="store_true")
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


async def _run_async_crawl(args: argparse.Namespace, *, project_root: Path) -> str:
    spec_path = resolve_spec_path(project_root / "specs", args.category)
    settings = Settings.from_env(project_root)
    spec = load_spec(spec_path)
    client = build_client(spec=spec, env_path=settings.env_path, use_async=True)
    store = AsyncPostgresStore(settings.database_url)
    reporter = TqdmProgressReporter(category=spec.category, verbose=args.verbose)
    crawler = AsyncSpecCrawler(
        base_url=base_url_for_spec(settings, spec),
        spec=spec,
        client=client,
        store=store,
        reporter=reporter,
        concurrency=args.concurrency,
    )
    try:
        return await crawler.run(
            CrawlOptions(
                mode=args.mode,
                from_datetime=args.from_datetime,
                to_datetime=args.to_datetime,
                max_pages=args.max_pages,
                batch_size=args.batch_size,
                page_size=args.page_size,
            )
        )
    finally:
        await store.close()


def main(argv: list[str] | None = None) -> int:
    project_root = Path(__file__).resolve().parents[1]
    args = parse_args(argv, project_root=project_root)
    try:
        spec_path = resolve_spec_path(project_root / "specs", args.category)
    except FileNotFoundError as exc:
        raise SystemExit(str(exc)) from exc
    if args.use_async:
        run_id = asyncio.run(_run_async_crawl(args, project_root=project_root))
        print(f"completed crawl run {run_id}")
        return 0
    settings = Settings.from_env(project_root)
    spec = load_spec(spec_path)
    client = build_client(spec=spec, env_path=settings.env_path, use_async=False)
    store = PostgresStore(settings.database_url)
    reporter = TqdmProgressReporter(category=spec.category, verbose=args.verbose)
    crawler = SpecCrawler(
        base_url=base_url_for_spec(settings, spec),
        spec=spec,
        client=client,
        store=store,
        reporter=reporter,
    )
    run_id = crawler.run(
        CrawlOptions(
            mode=args.mode,
            from_datetime=args.from_datetime,
            to_datetime=args.to_datetime,
            max_pages=args.max_pages,
            batch_size=args.batch_size,
            page_size=args.page_size,
        )
    )
    print(f"completed crawl run {run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
