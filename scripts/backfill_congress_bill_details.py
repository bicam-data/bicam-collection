"""Fetch Congress.gov details for bills that exist only in GovInfo.

Skips stored responses and known 404s, so it resumes safely.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.targeted_backfill import (
    TargetedCandidate,
    TargetedFamily,
    run_targeted_backfill,
)
from bicam_collection.urls import canonicalize_congress_api_url


FAMILY = TargetedFamily(
    category="bills",
    node_type="bill_detail",
    endpoint_name="congress-bill-detail-targeted-backfill",
    endpoint_priority=3,
    run_mode="targeted_bill_detail_backfill",
)


@dataclass(frozen=True, slots=True)
class CongressBillDetail:
    parent_raw_id: UUID
    parent_endpoint_url: str
    bill_type: str
    bill_number: int
    congress: int

    @property
    def endpoint_url(self) -> str:
        return (
            f"https://api.congress.gov/v3/bill/{self.congress}/{self.bill_type}/"
            f"{self.bill_number}?format=json"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch missing Congress.gov bill details identified by reconciliation."
    )
    parser.add_argument("--congress", type=int, action="append", help="Congress number; repeatable.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum bill details per run.")
    parser.add_argument("--concurrency", type=int, default=2, help="Maximum simultaneous requests.")
    parser.add_argument("--request-interval", type=float, default=2.0)
    parser.add_argument("--key-wait-timeout", type=float, default=180.0)
    parser.add_argument("--refresh", action="store_true", help="Refetch details already stored locally.")
    return parser.parse_args(argv)


def load_bill_details(
    *, database_url: str, congresses: list[int] | None, limit: int, refresh: bool
) -> list[CongressBillDetail]:
    congress_filter = ""
    params: list[Any] = []
    if congresses:
        congress_filter = "and govinfo.congress = any(%s)"
        params.append(congresses)

    existing_filter = ""
    if not refresh:
        existing_filter = """
        and not exists (
            select 1
            from raw_responses as existing
            where existing.endpoint_url =
                'https://api.congress.gov/v3/bill/'
                || govinfo.congress::text
                || '/'
                || govinfo.bill_type
                || '/'
                || govinfo.bill_number::text
                || '?format=json'
              and existing.source = 'congress'
              and existing.category = 'bills'
              and existing.node_type = 'bill_detail'
        )
        and not exists (
            select 1
            from failed_requests as prior_failure
            where prior_failure.endpoint_url =
                'https://api.congress.gov/v3/bill/'
                || govinfo.congress::text
                || '/'
                || govinfo.bill_type
                || '/'
                || govinfo.bill_number::text
                || '?format=json'
              and prior_failure.category = 'bills'
              and prior_failure.node_type = 'bill_detail'
              and prior_failure.http_status = 404
        )
        """

    params.append(max(1, limit))
    query = f"""
        select
            govinfo.source_raw_id,
            govinfo.source_endpoint_url,
            govinfo.bill_type,
            govinfo.bill_number,
            govinfo.congress
        from int_bills__govinfo as govinfo
        where govinfo.source_raw_id is not null
          and govinfo.bill_type is not null
          and govinfo.bill_number is not null
          and govinfo.congress is not null
          and exists (
              select 1
              from fct_bill_matches as matches
              where matches.govinfo_source_record_id = govinfo.source_record_id
                and matches.decision = 'unmatched'
                and matches.decision_rule = 'missing_congress_record'
          )
          {congress_filter}
          {existing_filter}
        order by govinfo.congress desc, govinfo.bill_type, govinfo.bill_number
        limit %s
    """
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        CongressBillDetail(
            parent_raw_id=row[0],
            parent_endpoint_url=row[1],
            bill_type=row[2],
            bill_number=row[3],
            congress=row[4],
        )
        for row in rows
    ]


async def run_backfill(args: argparse.Namespace, *, project_root: Path) -> tuple[int, int]:
    settings = Settings.from_env(project_root)
    bill_details = load_bill_details(
        database_url=settings.database_url,
        congresses=args.congress,
        limit=args.limit,
        refresh=args.refresh,
    )
    if not bill_details:
        print("no Congress bill details need backfill")
        return 0, 0

    spec = load_spec(project_root / "specs/bills.yaml")
    base_url = base_url_for_spec(settings, spec)
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=max(0.0, args.request_interval),
    )
    store = AsyncPostgresStore(settings.database_url)

    async def traverse_children(candidate: TargetedCandidate) -> None:
        # Walk this stored detail now so its missing children are fetched without
        # waiting for the partition crawler.
        descendant_crawler = AsyncSpecCrawler(
            base_url=base_url,
            spec=spec,
            client=client,
            store=store,
            concurrency=1,
        )
        await descendant_crawler.run(
            CrawlOptions(
                mode="backfill_descendants",
                seed_path=candidate.endpoint_url,
                seed_node_type=FAMILY.node_type,
                max_pages=1000,
                page_size=250,
            )
        )

    candidates = [
        TargetedCandidate(
            endpoint_url=canonicalize_congress_api_url(bill.endpoint_url),
            parent_id=str(bill.parent_raw_id),
            parent_endpoint_url=bill.parent_endpoint_url,
        )
        for bill in bill_details
    ]
    outcome = await run_targeted_backfill(
        FAMILY,
        candidates,
        client=client,
        store=store,
        concurrency=args.concurrency,
        after_success=traverse_children,
    )
    print(
        f"completed targeted bill detail backfill {outcome.run_id}: "
        f"fetched={outcome.fetched} failed={outcome.failed}"
    )
    return outcome.fetched, outcome.failed


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit < 1 or args.concurrency < 1:
        raise SystemExit("limit and concurrency must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0:
        raise SystemExit("request-interval must be non-negative and key-wait-timeout must be positive")
    asyncio.run(run_backfill(args, project_root=Path(__file__).resolve().parents[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
