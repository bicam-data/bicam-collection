from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path
import time
from typing import Any

import psycopg

from bicam_collection.config import Settings
from bicam_collection.runtime import build_client
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.urls import canonicalize_govinfo_url


CATEGORY = "govinfo-committee-reports"
NODE_TYPE = "govinfo_committee_report_package_detail"


@dataclass(frozen=True, slots=True)
class ReportSet:
    report_type: str
    report_number: int
    congress: int

    @property
    def package_id(self) -> str:
        return f"CRPT-{self.congress}{self.report_type}{self.report_number}"

    @property
    def package_url(self) -> str:
        return f"https://api.govinfo.gov/packages/{self.package_id}/summary"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch only GovInfo committee-report packages represented in Congress.gov data."
    )
    parser.add_argument(
        "--congress",
        type=int,
        action="append",
        help="Congress number to backfill; repeatable. Defaults to all available Congresses, newest first.",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Maximum report sets to attempt per run.")
    parser.add_argument("--concurrency", type=int, default=3, help="Maximum simultaneous GovInfo requests.")
    parser.add_argument(
        "--granules-for-unmatched",
        action="store_true",
        help="Fetch granule details only for existing packages whose Congress.gov reports remain unmatched.",
    )
    parser.add_argument("--refresh", action="store_true", help="Refetch packages already stored locally.")
    return parser.parse_args(argv)


def load_report_sets(
    *, database_url: str, congresses: list[int] | None, limit: int, refresh: bool
) -> list[ReportSet]:
    congress_filter = ""
    params: list[Any] = []
    if congresses:
        congress_filter = "where reports.congress = any(%s)"
        params.append(congresses)
    params.append(max(1, limit))
    existing_filter = ""
    if not refresh:
        existing_filter = """
        where not exists (
            select 1
            from raw_responses as existing
            where existing.endpoint_url =
                'https://api.govinfo.gov/packages/CRPT-'
                || report_sets.congress::text
                || report_sets.report_type
                || report_sets.report_number::text
                || '/summary'
        )
        """
    query = f"""
        with report_sets as (
            select distinct reports.report_type, reports.report_number, reports.congress
            from int_committee_reports__congress as reports
            {congress_filter}
        )
        select report_type, report_number, congress
        from report_sets
        {existing_filter}
        order by congress desc, report_type, report_number
        limit %s
    """
    with psycopg.connect(database_url) as conn:
        rows = conn.execute(query, params).fetchall()
    return [ReportSet(report_type=row[0], report_number=row[1], congress=row[2]) for row in rows]


def load_unmatched_granule_list_urls(*, database_url: str, limit: int) -> list[str]:
    """Return package granule lists only where a non-package report part remains unmatched."""
    query = """
        with unmatched_report_sets as (
            select distinct congress.congress, congress.report_type, congress.report_number
            from int_committee_reports__congress as congress
            join fct_committee_report_matches as matches
              on matches.congress_source_record_id = congress.source_record_id
            where matches.decision = 'unmatched'
              and exists (
                  select 1
                  from int_committee_reports__govinfo as govinfo
                  where govinfo.congress = congress.congress
                    and govinfo.report_type = congress.report_type
                    and govinfo.report_number = congress.report_number
              )
        )
        select distinct packages.response ->> 'granulesLink' as granules_url
        from raw_responses as packages
        join unmatched_report_sets as report_sets
          on packages.endpoint_url =
              'https://api.govinfo.gov/packages/CRPT-'
              || report_sets.congress::text
              || report_sets.report_type
              || report_sets.report_number::text
              || '/summary'
        where packages.source = 'govinfo'
          and packages.category = %s
          and packages.node_type = %s
          and packages.response ? 'granulesLink'
        order by granules_url
        limit %s
    """
    with psycopg.connect(database_url) as conn:
        rows = conn.execute(query, (CATEGORY, NODE_TYPE, max(1, limit))).fetchall()
    return [canonicalize_govinfo_url(row[0]) for row in rows if row[0]]


def load_existing_granule_detail_urls(*, database_url: str) -> set[str]:
    query = """
        select endpoint_url
        from raw_responses
        where source = 'govinfo'
          and category = %s
          and node_type = 'govinfo_committee_report_granule_detail'
    """
    with psycopg.connect(database_url) as conn:
        return {row[0] for row in conn.execute(query, (CATEGORY,)).fetchall()}


def _metadata_values(metadata: Any) -> dict[str, Any]:
    return {
        "status_code": getattr(metadata, "status_code", None),
        "duration_ms": getattr(metadata, "duration_ms", None),
        "attempt_count": getattr(metadata, "attempt_count", None),
        "rate_limit_attempts": getattr(metadata, "rate_limit_attempts", None),
        "api_key_name": getattr(metadata, "api_key_name", None),
        "pool_allocation": getattr(metadata, "pool_allocation", None),
    }


async def run_backfill(args: argparse.Namespace, *, project_root: Path) -> tuple[int, int]:
    settings = Settings.from_env(project_root)
    if args.granules_for_unmatched:
        return await run_unmatched_granule_backfill(args, settings=settings)
    report_sets = load_report_sets(
        database_url=settings.database_url,
        congresses=args.congress,
        limit=args.limit,
        refresh=args.refresh,
    )
    if not report_sets:
        print("no report packages need backfill")
        return 0, 0

    spec = load_spec(project_root / "specs" / "govinfo-committee-reports.yaml")
    client = build_client(spec=spec, env_path=settings.env_path, use_async=True)
    store = AsyncPostgresStore(settings.database_url)
    run_id = await store.start_run(CATEGORY, "targeted_package_backfill")
    semaphore = asyncio.Semaphore(max(1, args.concurrency))
    succeeded = 0
    failed = 0
    counter_lock = asyncio.Lock()

    async def fetch(report_set: ReportSet) -> None:
        nonlocal succeeded, failed
        endpoint_url = canonicalize_govinfo_url(report_set.package_url)
        started_at = time.perf_counter()
        try:
            async with semaphore:
                result = await client.get_json(
                    endpoint_url,
                    endpoint_name="govinfo-committee-report-targeted-backfill",
                    endpoint_priority=3,
                )
            metadata = _metadata_values(result.metadata)
            raw_id = await store.upsert_response(
                endpoint_url=endpoint_url,
                source="govinfo",
                category=CATEGORY,
                node_type=NODE_TYPE,
                parent_id=None,
                response=result.payload,
                latest_crawl_run_id=run_id,
            )
            await store.record_fetch(
                crawl_run_id=run_id,
                endpoint_url=endpoint_url,
                raw_response_id=raw_id,
                parent_endpoint_url=None,
                node_type=NODE_TYPE,
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
                succeeded += 1
        except Exception as exc:
            response = getattr(exc, "response", None)
            metadata = _metadata_values(getattr(exc, "fetch_metadata", None))
            duration_ms = metadata["duration_ms"]
            if duration_ms is None:
                duration_ms = round((time.perf_counter() - started_at) * 1000)
            await store.record_failed_request(
                crawl_run_id=run_id,
                category=CATEGORY,
                node_type=NODE_TYPE,
                endpoint_url=endpoint_url,
                parent_endpoint_url=None,
                http_status=getattr(response, "status_code", metadata["status_code"]),
                error=str(exc),
                duration_ms=duration_ms,
                attempt_count=metadata["attempt_count"],
                rate_limit_attempts=metadata["rate_limit_attempts"],
                api_key_name=metadata["api_key_name"],
                pool_allocation=metadata["pool_allocation"],
            )
            async with counter_lock:
                failed += 1

    try:
        await asyncio.gather(*(fetch(report_set) for report_set in report_sets))
        await store.finish_run(run_id, "completed", succeeded)
    except BaseException:
        await store.finish_run(run_id, "failed", succeeded)
        raise
    finally:
        await store.close()
    print(f"completed targeted package backfill {run_id}: fetched={succeeded} failed={failed}")
    return succeeded, failed


async def run_unmatched_granule_backfill(args: argparse.Namespace, *, settings: Settings) -> tuple[int, int]:
    granule_list_urls = load_unmatched_granule_list_urls(database_url=settings.database_url, limit=args.limit)
    if not granule_list_urls:
        print("no unmatched report packages need granule backfill")
        return 0, 0

    existing_detail_urls = set() if args.refresh else load_existing_granule_detail_urls(database_url=settings.database_url)
    spec = load_spec(Path(settings.env_path).parent / "specs" / "govinfo-committee-reports.yaml")
    client = build_client(spec=spec, env_path=settings.env_path, use_async=True)
    store = AsyncPostgresStore(settings.database_url)
    run_id = await store.start_run(CATEGORY, "targeted_unmatched_granule_backfill")
    semaphore = asyncio.Semaphore(max(1, args.concurrency))
    succeeded = 0
    failed = 0
    counter_lock = asyncio.Lock()

    async def fetch_and_store(endpoint_url: str, *, node_type: str, parent_endpoint_url: str | None) -> dict[str, Any] | None:
        nonlocal succeeded, failed
        endpoint_url = canonicalize_govinfo_url(endpoint_url)
        started_at = time.perf_counter()
        try:
            async with semaphore:
                result = await client.get_json(endpoint_url, endpoint_name="govinfo-committee-report-targeted-granule-backfill", endpoint_priority=3)
            metadata = _metadata_values(result.metadata)
            raw_id = await store.upsert_response(endpoint_url=endpoint_url, source="govinfo", category=CATEGORY, node_type=node_type, parent_id=None, response=result.payload, latest_crawl_run_id=run_id)
            await store.record_fetch(crawl_run_id=run_id, endpoint_url=endpoint_url, raw_response_id=raw_id, parent_endpoint_url=parent_endpoint_url, node_type=node_type, follow_mode="targeted", http_status=metadata["status_code"], outcome="fetched", duration_ms=metadata["duration_ms"], attempt_count=metadata["attempt_count"], rate_limit_attempts=metadata["rate_limit_attempts"], api_key_name=metadata["api_key_name"], pool_allocation=metadata["pool_allocation"])
            async with counter_lock:
                succeeded += 1
            return result.payload
        except Exception as exc:
            response = getattr(exc, "response", None)
            metadata = _metadata_values(getattr(exc, "fetch_metadata", None))
            duration_ms = metadata["duration_ms"] or round((time.perf_counter() - started_at) * 1000)
            await store.record_failed_request(crawl_run_id=run_id, category=CATEGORY, node_type=node_type, endpoint_url=endpoint_url, parent_endpoint_url=parent_endpoint_url, http_status=getattr(response, "status_code", metadata["status_code"]), error=str(exc), duration_ms=duration_ms, attempt_count=metadata["attempt_count"], rate_limit_attempts=metadata["rate_limit_attempts"], api_key_name=metadata["api_key_name"], pool_allocation=metadata["pool_allocation"])
            async with counter_lock:
                failed += 1
            return None

    async def fetch_granule_list(granules_url: str) -> None:
        next_url: str | None = granules_url
        while next_url:
            payload = await fetch_and_store(next_url, node_type="govinfo_committee_report_granule_list", parent_endpoint_url=None)
            if not payload:
                return
            detail_urls = [canonicalize_govinfo_url(granule["granuleLink"]) for granule in payload.get("granules", []) if granule.get("granuleLink")]
            await asyncio.gather(*(fetch_and_store(detail_url, node_type="govinfo_committee_report_granule_detail", parent_endpoint_url=next_url) for detail_url in detail_urls if detail_url not in existing_detail_urls))
            next_url = payload.get("nextPage")

    try:
        await asyncio.gather(*(fetch_granule_list(url) for url in granule_list_urls))
        await store.finish_run(run_id, "completed", succeeded)
    except BaseException:
        await store.finish_run(run_id, "failed", succeeded)
        raise
    finally:
        await store.close()
    print(f"completed targeted granule backfill {run_id}: fetched={succeeded} failed={failed}")
    return succeeded, failed


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    asyncio.run(run_backfill(args, project_root=Path(__file__).resolve().parents[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
