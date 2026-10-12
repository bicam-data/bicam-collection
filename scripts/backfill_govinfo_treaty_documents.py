"""Fetch GovInfo CDOC packages and granules for the treaties known to Congress.gov.

Treaty package IDs are deterministic, so only those packages are fetched;
stored ones are walked for descendants and recorded as skipped.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import math
from pathlib import Path
import time
from typing import Any

import psycopg

from bicam_collection.client import fetch_metadata_values
from bicam_collection.config import Settings
from bicam_collection.runtime import build_client
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import AsyncPostgresStore, StoredNode
from bicam_collection.urls import canonicalize_govinfo_url, with_query_params


CATEGORY = "govinfo-treaty-documents"
PACKAGE_NODE_TYPE = "govinfo_treaty_document_package_detail"
GRANULE_LIST_NODE_TYPE = "govinfo_treaty_document_granule_list"
GRANULE_DETAIL_NODE_TYPE = "govinfo_treaty_document_granule_detail"
GRANULE_PAGE_SIZE = 100


@dataclass(frozen=True, slots=True)
class TreatyPackage:
    congress_received: int
    treaty_number: int

    @property
    def package_id(self) -> str:
        return f"CDOC-{self.congress_received}tdoc{self.treaty_number}"

    @property
    def package_url(self) -> str:
        return f"https://api.govinfo.gov/packages/{self.package_id}/summary"


def treaty_package_id(*, congress_received: int, treaty_number: int) -> str:
    """Build GovInfo's deterministic CDOC treaty-document package ID."""

    if congress_received < 1 or treaty_number < 1:
        raise ValueError("congress_received and treaty_number must be positive")
    return f"CDOC-{congress_received}tdoc{treaty_number}"


def _granule_list_url(url: str) -> str:
    return canonicalize_govinfo_url(
        with_query_params(url, {"offsetMark": "*", "pageSize": GRANULE_PAGE_SIZE})
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--congress",
        type=int,
        action="append",
        help="Congress number to target; repeatable. Defaults to newest first.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum treaty packages to target in this bounded run (default: 100).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="Maximum granule-list pages followed per package (default: 100).",
    )
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument(
        "--request-interval",
        type=float,
        default=0.25,
        help="Minimum seconds between GovInfo requests from this client.",
    )
    parser.add_argument(
        "--key-wait-timeout",
        type=float,
        default=180.0,
        help="Maximum seconds to wait for an available GovInfo key.",
    )
    parser.add_argument("--refresh", action="store_true", help="Refetch stored package and granule responses.")
    parser.add_argument(
        "--retry-404-after-hours",
        type=float,
        default=168.0,
        help="Retry a prior GovInfo 404 after this many hours (default: 168).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the bounded package targets without making HTTP requests or writes.",
    )
    return parser.parse_args(argv)


def load_treaty_packages(
    *,
    database_url: str,
    congresses: list[int] | None,
    limit: int,
    refresh: bool,
    retry_404_after_hours: float = 168.0,
) -> list[TreatyPackage]:
    """Return Congress.gov treaty identities needing a CDOC package probe."""

    if limit < 1:
        raise ValueError("limit must be positive")
    if not math.isfinite(retry_404_after_hours) or retry_404_after_hours <= 0:
        raise ValueError("retry_404_after_hours must be positive and finite")

    clauses = [
        "treaties.congress_received is not null",
        "treaties.treaty_number is not null",
        "treaties.congress_received > 0",
        "treaties.treaty_number > 0",
    ]
    params: list[Any] = []
    if congresses:
        clauses.append("treaties.congress_received = any(%s)")
        params.append(congresses)

    package_url = (
        "'https://api.govinfo.gov/packages/CDOC-' || treaties.congress_received::text"
        " || 'tdoc' || treaties.treaty_number::text || '/summary'"
    )
    if not refresh:
        clauses.extend(
            [
                f"not exists (select 1 from raw_responses where endpoint_url = {package_url}"
                " and source = 'govinfo' and category = 'govinfo-treaty-documents'"
                " and node_type = 'govinfo_treaty_document_package_detail')",
                f"not exists (select 1 from failed_requests as prior_failure"
                f" where prior_failure.endpoint_url = {package_url}"
                " and prior_failure.category = 'govinfo-treaty-documents'"
                " and prior_failure.node_type = 'govinfo_treaty_document_package_detail'"
                " and prior_failure.http_status = 404"
                f" and prior_failure.failed_at >= now() - interval '{retry_404_after_hours:.6f} hours')",
            ]
        )

    query = f"""
        select distinct treaties.congress_received, treaties.treaty_number
        from stg_congress__treaty_detail as treaties
        where {' and '.join(clauses)}
        order by treaties.congress_received desc, treaties.treaty_number
        limit %s
    """
    params.append(max(1, limit))
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(query, params).fetchall()
    return [TreatyPackage(congress_received=int(row[0]), treaty_number=int(row[1])) for row in rows]


def _granule_detail_urls(payload: dict[str, Any]) -> list[str]:
    """Extract and canonicalize granule detail links from a GovInfo page."""

    urls: list[str] = []
    for granule in payload.get("granules", []) or []:
        if not isinstance(granule, dict):
            continue
        link = granule.get("granuleLink")
        if isinstance(link, str) and link.strip():
            urls.append(canonicalize_govinfo_url(link))
    return urls


def _next_page(payload: dict[str, Any]) -> str | None:
    value = payload.get("nextPage")
    return canonicalize_govinfo_url(value) if isinstance(value, str) and value.strip() else None


async def run_backfill(args: argparse.Namespace, *, project_root: Path) -> tuple[int, int]:
    if args.limit < 1 or args.max_pages < 1 or args.concurrency < 1:
        raise ValueError("limit, max-pages, and concurrency must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0:
        raise ValueError("request-interval must be non-negative and key-wait-timeout must be positive")
    settings = Settings.from_env(project_root)
    packages = load_treaty_packages(
        database_url=settings.database_url,
        congresses=args.congress,
        limit=args.limit,
        refresh=args.refresh,
        retry_404_after_hours=args.retry_404_after_hours,
    )
    print(f"[govinfo-treaty-documents] package_targets={len(packages)}", flush=True)
    if args.dry_run:
        for package in packages:
            print(f"would target {package.package_id}: {package.package_url}", flush=True)
        return 0, 0
    if not packages:
        return 0, 0

    spec = load_spec(project_root / "specs/govinfo-treaty-documents.yaml")
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=max(0.0, args.request_interval),
    )
    store = AsyncPostgresStore(settings.database_url)
    run_id = await store.start_run(CATEGORY, "targeted_treaty_document_backfill")
    semaphore = asyncio.Semaphore(args.concurrency)
    seen: set[str] = set()
    seen_lock = asyncio.Lock()
    stored_cache: dict[str, StoredNode | None] = {}
    stored_lock = asyncio.Lock()
    counters = {"fetched": 0, "failed": 0, "skipped": 0}
    counter_lock = asyncio.Lock()

    async def lookup_stored(url: str) -> StoredNode | None:
        if args.refresh:
            return None
        async with stored_lock:
            if url not in stored_cache:
                stored_cache[url] = await store.get_stored_node(url)
            return stored_cache[url]

    async def mark_expected(url: str, parent_url: str | None, node_type: str) -> None:
        await store.record_expected_fetch(
            crawl_run_id=run_id,
            endpoint_url=url,
            parent_endpoint_url=parent_url,
            node_type=node_type,
            follow_mode="targeted",
        )

    async def mark_skip(
        url: str,
        parent_url: str | None,
        node_type: str,
        *,
        outcome: str,
        stored: StoredNode | None = None,
    ) -> None:
        await store.record_fetch(
            crawl_run_id=run_id,
            endpoint_url=url,
            raw_response_id=stored.raw_id if stored and outcome == "skipped_existing_backfill" else None,
            raw_response_version_id=(
                stored.raw_response_version_id
                if stored and outcome == "skipped_existing_backfill"
                else None
            ),
            parent_endpoint_url=parent_url,
            node_type=node_type,
            follow_mode="targeted",
            http_status=None,
            outcome=outcome,
        )
        async with counter_lock:
            counters["skipped"] += 1

    async def fetch_node(
        url: str,
        *,
        node_type: str,
        parent_url: str | None,
        page_number: int = 1,
    ) -> dict[str, Any] | None:
        url = canonicalize_govinfo_url(url)
        await mark_expected(url, parent_url, node_type)
        async with seen_lock:
            if url in seen:
                duplicate = True
            else:
                seen.add(url)
                duplicate = False
        if duplicate:
            await mark_skip(url, parent_url, node_type, outcome="skipped_seen")
            return None

        stored = await lookup_stored(url)
        if stored is not None:
            await mark_skip(url, parent_url, node_type, outcome="skipped_existing_backfill", stored=stored)
            return stored.response

        started_at = time.perf_counter()
        try:
            async with semaphore:
                result = await client.get_json(
                    url,
                    endpoint_name=f"govinfo-treaty-document-{node_type}",
                    endpoint_priority=3,
                )
            metadata = fetch_metadata_values(result.metadata)
            response_ref = await store.upsert_response(
                endpoint_url=url,
                source="govinfo",
                category=CATEGORY,
                node_type=node_type,
                parent_id=None,
                response=result.payload,
                latest_crawl_run_id=run_id,
            )
            await store.record_fetch(
                crawl_run_id=run_id,
                endpoint_url=url,
                raw_response_id=response_ref.raw_response_id,
                raw_response_version_id=response_ref.raw_response_version_id,
                parent_endpoint_url=parent_url,
                node_type=node_type,
                follow_mode="targeted",
                http_status=metadata["status_code"],
                outcome="fetched",
                duration_ms=metadata["duration_ms"],
                attempt_count=metadata["attempt_count"],
                rate_limit_attempts=metadata["rate_limit_attempts"],
                api_key_name=metadata["api_key_name"],
                pool_allocation=metadata["pool_allocation"],
            )
            async with counter_lock:
                counters["fetched"] += 1
            return result.payload
        except Exception as exc:
            response = getattr(exc, "response", None)
            metadata = fetch_metadata_values(getattr(exc, "fetch_metadata", None))
            duration_ms = metadata["duration_ms"]
            if duration_ms is None:
                duration_ms = max(1, round((time.perf_counter() - started_at) * 1000))
            await store.record_failed_request(
                crawl_run_id=run_id,
                category=CATEGORY,
                node_type=node_type,
                endpoint_url=url,
                parent_endpoint_url=parent_url,
                http_status=getattr(response, "status_code", metadata["status_code"]),
                error=str(exc),
                duration_ms=duration_ms,
                attempt_count=metadata["attempt_count"],
                rate_limit_attempts=metadata["rate_limit_attempts"],
                api_key_name=metadata["api_key_name"],
                pool_allocation=metadata["pool_allocation"],
            )
            async with counter_lock:
                counters["failed"] += 1
            return None

    async def process_package(package: TreatyPackage) -> None:
        package_url = canonicalize_govinfo_url(package.package_url)
        payload = await fetch_node(package_url, node_type=PACKAGE_NODE_TYPE, parent_url=None)
        if not payload:
            return
        granules_link = payload.get("granulesLink")
        if not isinstance(granules_link, str) or not granules_link.strip():
            return
        await process_granule_page(_granule_list_url(granules_link), package_url, 1)

    async def process_granule_page(url: str, package_url: str, page_number: int) -> None:
        if page_number > args.max_pages:
            return
        payload = await fetch_node(
            url,
            node_type=GRANULE_LIST_NODE_TYPE,
            parent_url=package_url,
            page_number=page_number,
        )
        if not payload:
            return
        detail_urls = _granule_detail_urls(payload)
        next_url = _next_page(payload)
        await asyncio.gather(
            *(fetch_node(detail, node_type=GRANULE_DETAIL_NODE_TYPE, parent_url=url) for detail in detail_urls)
        )
        if next_url and page_number < args.max_pages:
            await process_granule_page(next_url, package_url, page_number + 1)

    try:
        await asyncio.gather(*(process_package(package) for package in packages))
        await store.finish_run(run_id, "completed", counters["fetched"])
    except BaseException:
        await store.finish_run(run_id, "failed", counters["fetched"])
        raise
    finally:
        await store.close()
    print(
        f"completed targeted treaty-document backfill {run_id}: "
        f"fetched={counters['fetched']} failed={counters['failed']} skipped={counters['skipped']}",
        flush=True,
    )
    return counters["fetched"], counters["failed"]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        asyncio.run(run_backfill(args, project_root=Path(__file__).resolve().parents[1]))
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
