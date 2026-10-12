from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from bicam_collection.config import Settings
from bicam_collection.runtime import build_client
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
    node_type="bill_text",
    endpoint_name="congress-bill-text-targeted-backfill",
    endpoint_priority=3,
    run_mode="targeted_bill_text_backfill",
)


@dataclass(frozen=True, slots=True)
class BillText:
    parent_raw_id: UUID
    parent_endpoint_url: str
    bill_type: str
    bill_number: int
    congress: int

    @property
    def endpoint_url(self) -> str:
        return (
            f"https://api.congress.gov/v3/bill/{self.congress}/{self.bill_type}/"
            f"{self.bill_number}/text?format=json&limit=250"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch missing Congress.gov bill text-version endpoints for collected bill details."
    )
    parser.add_argument("--congress", type=int, action="append", help="Congress number to backfill; repeatable.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum bill text endpoints to attempt per run.")
    parser.add_argument("--concurrency", type=int, default=3, help="Maximum simultaneous Congress.gov requests.")
    parser.add_argument("--refresh", action="store_true", help="Refetch text endpoints already stored locally.")
    return parser.parse_args(argv)


def load_bill_texts(
    *, database_url: str, congresses: list[int] | None, limit: int, refresh: bool
) -> list[BillText]:
    congress_filter = ""
    params: list[Any] = []
    if congresses:
        congress_filter = "and bills.congress = any(%s)"
        params.append(congresses)
    existing_filter = ""
    if not refresh:
        existing_filter = """
        and not exists (
            select 1
            from raw_responses as texts
            where texts.endpoint_url =
                'https://api.congress.gov/v3/bill/'
                || bills.congress::text
                || '/'
                || bills.bill_type
                || '/'
                || bills.bill_number::text
                || '/text?format=json&limit=250'
              and texts.source = 'congress'
              and texts.category = 'bills'
              and texts.node_type = 'bill_text'
        )
        """
    params.append(max(1, limit))
    query = f"""
        select
            bills.raw_id,
            bills.endpoint_url,
            bills.bill_type,
            bills.bill_number,
            bills.congress
        from stg_congress__bill_detail as bills
        where bills.texts_count > 0
        {congress_filter}
        {existing_filter}
        order by bills.congress desc, bills.bill_type, bills.bill_number
        limit %s
    """
    with psycopg.connect(database_url) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        BillText(
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
    bill_texts = load_bill_texts(
        database_url=settings.database_url,
        congresses=args.congress,
        limit=args.limit,
        refresh=args.refresh,
    )
    if not bill_texts:
        print("no bill text endpoints need backfill")
        return 0, 0

    spec = load_spec(project_root / "specs" / "bills.yaml")
    client = build_client(spec=spec, env_path=settings.env_path, use_async=True)
    store = AsyncPostgresStore(settings.database_url)
    candidates = [
        TargetedCandidate(
            endpoint_url=canonicalize_congress_api_url(bill_text.endpoint_url),
            parent_id=str(bill_text.parent_raw_id),
            parent_endpoint_url=bill_text.parent_endpoint_url,
        )
        for bill_text in bill_texts
    ]
    # This script does not validate its arguments, so clamp here rather than
    # letting a zero concurrency deadlock the semaphore.
    outcome = await run_targeted_backfill(
        FAMILY,
        candidates,
        client=client,
        store=store,
        concurrency=max(1, args.concurrency),
    )
    print(
        f"completed targeted bill text backfill {outcome.run_id}: "
        f"fetched={outcome.fetched} failed={outcome.failed}"
    )
    return outcome.fetched, outcome.failed


def main(argv: list[str] | None = None) -> int:
    asyncio.run(run_backfill(parse_args(argv), project_root=Path(__file__).resolve().parents[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
