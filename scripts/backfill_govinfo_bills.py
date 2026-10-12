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
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.urls import canonicalize_govinfo_url


CATEGORY = "govinfo-bills"
NODE_TYPE = "govinfo_bill_package_detail"


@dataclass(frozen=True, slots=True)
class BillPackage:
    package_id: str

    @property
    def package_url(self) -> str:
        return f"https://api.govinfo.gov/packages/{self.package_id}/summary"


def introduced_package_id(*, bill_type: str, bill_number: int, congress: int) -> str:
    """GovInfo package ID of the introduced version (``ih`` House, ``is`` Senate)."""

    normalized_type = bill_type.lower()
    senate_types = {"s", "sconres", "sjres", "sres"}
    suffix = "is" if normalized_type in senate_types else "ih"
    return f"BILLS-{congress}{normalized_type}{bill_number}{suffix}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch only GovInfo BILLS packages referenced by Congress.gov bill text versions."
    )
    parser.add_argument("--congress", type=int, action="append", help="Congress number to backfill; repeatable.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum package summaries to attempt per run.")
    parser.add_argument("--concurrency", type=int, default=3, help="Maximum simultaneous GovInfo requests.")
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
        help="Maximum seconds to wait for a rate-limited GovInfo API key.",
    )
    parser.add_argument("--refresh", action="store_true", help="Refetch package summaries already stored locally.")
    parser.add_argument(
        "--unmatched",
        action="store_true",
        help="Target introduced GovInfo package IDs for bills currently unmatched to GovInfo.",
    )
    parser.add_argument(
        "--modern-missing",
        action="store_true",
        help="Probe conventional GovInfo packages for modern Congress-only bill records.",
    )
    parser.add_argument(
        "--retry-404-after-hours",
        type=float,
        default=168.0,
        help="Re-probe a prior GovInfo 404 after this many hours (default: 168).",
    )
    return parser.parse_args(argv)


def load_bill_packages(
    *,
    database_url: str,
    congresses: list[int] | None,
    limit: int,
    refresh: bool,
    unmatched: bool = False,
    modern_missing: bool = False,
    retry_404_after_hours: float = 168.0,
) -> list[BillPackage]:
    if unmatched and modern_missing:
        raise ValueError("unmatched and modern_missing are mutually exclusive")
    if not math.isfinite(retry_404_after_hours) or retry_404_after_hours <= 0:
        raise ValueError("retry_404_after_hours must be positive")
    congress_filter = ""
    params: list[Any] = []
    if congresses:
        congress_filter = "where bills.congress = any(%s)"
        if not unmatched and not modern_missing:
            params.append(congresses)
    existing_filter = ""
    if not refresh:
        retry_404_interval = f"interval '{retry_404_after_hours:.6f} hours'"
        existing_filter = f"""
        and not exists (
            select 1
            from raw_responses as existing
            where existing.endpoint_url =
                'https://api.govinfo.gov/packages/' || bill_packages.package_id || '/summary'
              and existing.source = 'govinfo'
              and existing.category = 'govinfo-bills'
              and existing.node_type = 'govinfo_bill_package_detail'
        )
        and not exists (
            select 1
            from failed_requests as prior_failure
            where prior_failure.endpoint_url =
                'https://api.govinfo.gov/packages/' || bill_packages.package_id || '/summary'
              and prior_failure.category = 'govinfo-bills'
              and prior_failure.node_type = 'govinfo_bill_package_detail'
              and prior_failure.http_status = 404
              and prior_failure.failed_at >= now() - {retry_404_interval}
        )
        """
    if unmatched:
        unmatched_congress_filter = ""
        if congresses:
            unmatched_congress_filter = "and parsed.congress = any(%s)"
            params.append(congresses)
        params.append(max(1, limit))
        query = f"""
        with parsed as (
            select distinct
                (regexp_match(exceptions.canonical_id, '^([a-z]+)([0-9]+)-([0-9]+)$'))[1] as bill_type,
                ((regexp_match(exceptions.canonical_id, '^([a-z]+)([0-9]+)-([0-9]+)$'))[2])::integer as bill_number,
                ((regexp_match(exceptions.canonical_id, '^([a-z]+)([0-9]+)-([0-9]+)$'))[3])::integer as congress
            from fct_reconciliation_exceptions as exceptions
            where exceptions.entity = 'bill'
              and exceptions.missing_source = 'govinfo'
              and exceptions.exception_status = 'open'
              and exceptions.canonical_id ~ '^[a-z]+[0-9]+-[0-9]+$'
        ), bill_packages as (
            select distinct
                'BILLS-' || congress || bill_type || bill_number
                || case when bill_type in ('s', 'sconres', 'sjres', 'sres') then 'is' else 'ih' end as package_id,
                congress
            from parsed
            where true
            {unmatched_congress_filter}
        )
        select package_id
        from bill_packages
        where true
        {existing_filter}
        order by package_id
        limit %s
        """
    elif modern_missing:
        modern_congress_filter = ""
        if congresses:
            modern_congress_filter = "and congresses.congress = any(%s)"
            params.append(congresses)
        params.append(max(1, limit))
        query = f"""
        with missing as (
            select distinct
                congresses.bill_type,
                congresses.bill_number,
                congresses.congress
            from int_bills__congress as congresses
            join fct_bill_matches as matches
              on matches.congress_source_record_id = congresses.source_record_id
            where matches.decision = 'unmatched'
              and matches.govinfo_source_record_id is null
              and congresses.congress >= 103
              and lower(coalesce(congresses.title, '')) not like 'reserved%%'
              {modern_congress_filter}
        ), bill_packages as (
            select distinct
                'BILLS-' || congress || bill_type || bill_number
                || case when bill_type in ('s', 'sconres', 'sjres', 'sres') then 'is' else 'ih' end as package_id
            from missing
        )
        select package_id
        from bill_packages
        where package_id is not null
        {existing_filter}
        order by package_id
        limit %s
        """
    else:
        params.append(max(1, limit))
        query = f"""
        with bill_packages as (
            select distinct
                'BILLS-' || regexp_replace(
                    lower(substring(coalesce(texts.formatted_text, texts.pdf, texts.xml) from '/(BILLS-[^/.?]+)')),
                    '^bills-',
                    ''
                ) as package_id,
                bills.congress
            from stg_congress__bill_texts as texts
            join stg_congress__bill_detail as bills
              on texts.bill_id = bills.bill_id
            {congress_filter}
        )
        select package_id
        from bill_packages
        where package_id is not null
        {existing_filter}
        order by package_id
        limit %s
        """
    with psycopg.connect(database_url) as conn:
        rows = conn.execute(query, params).fetchall()
    return [BillPackage(package_id=row[0]) for row in rows]


async def run_backfill(args: argparse.Namespace, *, project_root: Path) -> tuple[int, int]:
    settings = Settings.from_env(project_root)
    bill_packages = load_bill_packages(
        database_url=settings.database_url,
        congresses=args.congress,
        limit=args.limit,
        refresh=args.refresh,
        unmatched=args.unmatched,
        modern_missing=args.modern_missing,
        retry_404_after_hours=args.retry_404_after_hours,
    )
    if not bill_packages:
        print("no bill packages need backfill")
        return 0, 0

    spec = load_spec(project_root / "specs" / "govinfo-bills.yaml")
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=max(0.0, args.request_interval),
    )
    store = AsyncPostgresStore(settings.database_url)
    run_id = await store.start_run(CATEGORY, "targeted_package_backfill")
    semaphore = asyncio.Semaphore(max(1, args.concurrency))
    succeeded = 0
    failed = 0
    counter_lock = asyncio.Lock()

    async def fetch(bill_package: BillPackage) -> None:
        nonlocal succeeded, failed
        endpoint_url = canonicalize_govinfo_url(bill_package.package_url)
        started_at = time.perf_counter()
        try:
            async with semaphore:
                result = await client.get_json(
                    endpoint_url,
                    endpoint_name="govinfo-bill-targeted-backfill",
                    endpoint_priority=3,
                )
            metadata = fetch_metadata_values(result.metadata)
            response_ref = await store.upsert_response(
                endpoint_url=endpoint_url,
                source="govinfo",
                category=CATEGORY,
                node_type=NODE_TYPE,
                parent_id=None,
                response=result.payload,
                latest_crawl_run_id=run_id,
            )
            raw_id = response_ref.raw_response_id
            await store.record_fetch(
                crawl_run_id=run_id,
                endpoint_url=endpoint_url,
                raw_response_id=raw_id,
                parent_endpoint_url=None,
                node_type=NODE_TYPE,
                follow_mode="targeted",
                http_status=metadata["status_code"],
                outcome="fetched",
                raw_response_version_id=response_ref.raw_response_version_id,
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
            metadata = fetch_metadata_values(getattr(exc, "fetch_metadata", None))
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
        await asyncio.gather(*(fetch(bill_package) for bill_package in bill_packages))
        await store.finish_run(run_id, "completed", succeeded)
    except BaseException:
        await store.finish_run(run_id, "failed", succeeded)
        raise
    finally:
        await store.close()
    print(f"completed targeted bill package backfill {run_id}: fetched={succeeded} failed={failed}")
    return succeeded, failed


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit < 1 or args.concurrency < 1:
        raise SystemExit("limit and concurrency must be positive")
    if args.unmatched and args.modern_missing:
        raise SystemExit("--unmatched and --modern-missing cannot be used together")
    if (
        args.request_interval < 0
        or args.key_wait_timeout <= 0
        or not math.isfinite(args.retry_404_after_hours)
        or args.retry_404_after_hours <= 0
    ):
        raise SystemExit(
            "request-interval must be non-negative, key-wait-timeout must be positive, "
            "and retry-404-after-hours must be positive"
        )
    asyncio.run(run_backfill(args, project_root=Path(__file__).resolve().parents[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
