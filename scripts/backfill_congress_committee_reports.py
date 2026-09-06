"""Probe Congress.gov committee-report details missing from reconciliation.

GovInfo contains a small number of report records for which the normal
Congress.gov report list did not yield a detail URL.  This bounded helper
turns those rows into auditable, rate-limited detail requests.  It is safe to
rerun: existing responses and known 404s are excluded unless ``--refresh`` is
requested.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path
import time
from typing import Any

import psycopg

from bicam_collection.client import fetch_metadata_values
from bicam_collection.config import Settings
from bicam_collection.runtime import build_client
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.urls import canonicalize_congress_api_url


CATEGORY = "committee-reports"
NODE_TYPE = "committee_report_detail"


@dataclass(frozen=True, slots=True)
class MissingCommitteeReport:
    source_raw_id: str
    source_endpoint_url: str
    report_type: str
    report_number: int
    congress: int
    report_part: int
    is_errata: bool

    @property
    def endpoint_url(self) -> str:
        # Congress.gov's detail URL identifies the report set.  The payload's
        # report_part distinguishes multipart records when one is returned.
        return (
            f"https://api.congress.gov/v3/committee-report/{self.congress}/"
            f"{self.report_type.upper()}/{self.report_number}?format=json"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Congress.gov committee reports missing from reconciliation."
    )
    parser.add_argument("--congress", type=int, action="append", help="Congress number; repeatable.")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--request-interval", type=float, default=2.0)
    parser.add_argument("--key-wait-timeout", type=float, default=600.0)
    parser.add_argument("--active-key-budget", type=int, default=1)
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args(argv)


def _rows(
    *, database_url: str, congresses: list[int] | None, limit: int, refresh: bool
) -> list[MissingCommitteeReport]:
    congress_filter = ""
    params: list[Any] = []
    if congresses:
        congress_filter = "and govinfo.congress = any(%s)"
        params.append(congresses)

    existing_filter = ""
    if not refresh:
        existing_filter = """
        and (
            govinfo.is_errata
            or not exists (
                select 1 from raw_responses as existing
                where existing.endpoint_url =
                  'https://api.congress.gov/v3/committee-report/'
                  || govinfo.congress::text || '/'
                  || upper(govinfo.report_type) || '/'
                  || govinfo.report_number::text || '?format=json'
                  and existing.source = 'congress'
                  and existing.category = 'committee-reports'
                  and existing.node_type = 'committee_report_detail'
            )
        )
        and (
            govinfo.is_errata
            or not exists (
                select 1 from failed_requests as prior_failure
                where prior_failure.endpoint_url =
                  'https://api.congress.gov/v3/committee-report/'
                  || govinfo.congress::text || '/'
                  || upper(govinfo.report_type) || '/'
                  || govinfo.report_number::text || '?format=json'
                  and prior_failure.category = 'committee-reports'
                  and prior_failure.node_type = 'committee_report_detail'
                  and prior_failure.http_status = 404
            )
        )
        """

    params.append(max(1, limit))
    query = f"""
        select govinfo.source_raw_id,
               govinfo.source_endpoint_url,
               govinfo.report_type,
               govinfo.report_number,
               govinfo.congress,
               coalesce(govinfo.report_part, 1),
               coalesce(govinfo.is_errata, false)
        from int_committee_reports__govinfo as govinfo
        left join int_committee_reports__congress as congress
          on congress.canonical_report_id = govinfo.canonical_report_id
        left join (
            select distinct canonical_id
            from fct_reconciliation_exceptions
            where entity = 'committee_report'
              and exception_status = 'open'
        ) as open_exceptions
          on open_exceptions.canonical_id = govinfo.canonical_report_id
        where congress.canonical_report_id is null
          and govinfo.report_type in ('hrpt', 'srpt')
          and govinfo.report_number is not null
          and govinfo.congress is not null
          and not exists (
              select 1
              from reconciliation_source_gap_rules as gaps
              where gaps.entity = 'committee_report'
                and gaps.missing_source = 'congress'
                and gaps.disposition = 'confirmed_source_gap'
                and current_date <= gaps.expires_at::date
                and govinfo.congress between gaps.congress_min and gaps.congress_max
          )
          and not exists (
              select 1
              from reconciliation_exception_reviews as reviews
              where reviews.entity = 'committee_report'
                and reviews.canonical_id = govinfo.canonical_report_id
                and reviews.disposition = 'confirmed_source_gap'
                and (reviews.expires_at is null or reviews.expires_at::date >= current_date)
          )
          {congress_filter}
          {existing_filter}
        order by case when open_exceptions.canonical_id is not null then 0 else 1 end,
                 govinfo.congress desc, govinfo.report_type, govinfo.report_number,
                 govinfo.report_part
        limit %s
    """
    with psycopg.connect(database_url) as connection:
        raw_rows = connection.execute(query, params).fetchall()
    return [
        MissingCommitteeReport(
            source_raw_id=str(row[0]),
            source_endpoint_url=row[1],
            report_type=row[2].lower(),
            report_number=int(row[3]),
            congress=int(row[4]),
            report_part=int(row[5]),
            is_errata=bool(row[6]),
        )
        for row in raw_rows
    ]


async def run(args: argparse.Namespace, *, project_root: Path) -> tuple[int, int]:
    settings = Settings.from_env(project_root)
    reports = _rows(
        database_url=settings.database_url,
        congresses=args.congress,
        limit=args.limit,
        refresh=args.refresh,
    )
    if not reports:
        print("no missing Congress committee reports need probing")
        return 0, 0

    spec = load_spec(project_root / "specs/committee-reports.yaml")
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=max(0.0, args.request_interval),
        active_key_budget=max(1, args.active_key_budget),
    )
    store = AsyncPostgresStore(settings.database_url)
    run_id = await store.start_run(CATEGORY, "targeted_committee_report_backfill")
    semaphore = asyncio.Semaphore(max(1, args.concurrency))
    succeeded = 0
    failed = 0
    counter_lock = asyncio.Lock()

    async def fetch(report: MissingCommitteeReport) -> None:
        nonlocal succeeded, failed
        endpoint_url = canonicalize_congress_api_url(report.endpoint_url)
        started_at = time.perf_counter()
        try:
            async with semaphore:
                result = await client.get_json(
                    endpoint_url,
                    endpoint_name="congress-committee-report-targeted-backfill",
                    endpoint_priority=3,
                )
            metadata = fetch_metadata_values(result.metadata)
            response_ref = await store.upsert_response(
                endpoint_url=endpoint_url,
                source="congress",
                category=CATEGORY,
                node_type=NODE_TYPE,
                parent_id=report.source_raw_id,
                response=result.payload,
                latest_crawl_run_id=run_id,
            )
            await store.record_fetch(
                crawl_run_id=run_id,
                endpoint_url=endpoint_url,
                raw_response_id=response_ref.raw_response_id,
                raw_response_version_id=response_ref.raw_response_version_id,
                parent_endpoint_url=report.source_endpoint_url,
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
            metadata = fetch_metadata_values(getattr(exc, "fetch_metadata", None))
            duration_ms = metadata["duration_ms"]
            if duration_ms is None:
                duration_ms = round((time.perf_counter() - started_at) * 1000)
            await store.record_failed_request(
                crawl_run_id=run_id,
                category=CATEGORY,
                node_type=NODE_TYPE,
                endpoint_url=endpoint_url,
                parent_endpoint_url=report.source_endpoint_url,
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
        await asyncio.gather(*(fetch(report) for report in reports))
        await store.finish_run(run_id, "completed", succeeded)
    except BaseException:
        await store.finish_run(run_id, "failed", succeeded)
        raise
    finally:
        await store.close()
    print(f"completed targeted committee report backfill {run_id}: fetched={succeeded} failed={failed}")
    return succeeded, failed


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit < 1 or args.concurrency < 1 or args.active_key_budget < 1:
        raise SystemExit("limit, concurrency, and active-key-budget must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0:
        raise SystemExit("request-interval must be non-negative and key-wait-timeout must be positive")
    asyncio.run(run(args, project_root=Path(__file__).resolve().parents[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
