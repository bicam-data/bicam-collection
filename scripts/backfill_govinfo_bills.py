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


CATEGORY = "govinfo-bills"
NODE_TYPE = "govinfo_bill_package_detail"


@dataclass(frozen=True, slots=True)
class BillPackage:
    package_id: str

    @property
    def package_url(self) -> str:
        return f"https://api.govinfo.gov/packages/{self.package_id}/summary"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch only GovInfo BILLS packages referenced by Congress.gov bill text versions."
    )
    parser.add_argument("--congress", type=int, action="append", help="Congress number to backfill; repeatable.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum package summaries to attempt per run.")
    parser.add_argument("--concurrency", type=int, default=3, help="Maximum simultaneous GovInfo requests.")
    parser.add_argument("--refresh", action="store_true", help="Refetch package summaries already stored locally.")
    return parser.parse_args(argv)


def load_bill_packages(
    *, database_url: str, congresses: list[int] | None, limit: int, refresh: bool
) -> list[BillPackage]:
    congress_filter = ""
    params: list[Any] = []
    if congresses:
        congress_filter = "where bills.congress = any(%s)"
        params.append(congresses)
    existing_filter = ""
    if not refresh:
        existing_filter = """
        and not exists (
            select 1
            from raw_responses as existing
            where existing.endpoint_url =
                'https://api.govinfo.gov/packages/' || bill_packages.package_id || '/summary'
              and existing.source = 'govinfo'
              and existing.category = 'govinfo-bills'
              and existing.node_type = 'govinfo_bill_package_detail'
        )
        """
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
    bill_packages = load_bill_packages(
        database_url=settings.database_url,
        congresses=args.congress,
        limit=args.limit,
        refresh=args.refresh,
    )
    if not bill_packages:
        print("no bill packages need backfill")
        return 0, 0

    spec = load_spec(project_root / "specs" / "govinfo-bills.yaml")
    client = build_client(spec=spec, env_path=settings.env_path, use_async=True)
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
            metadata = _metadata_values(result.metadata)
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
    asyncio.run(run_backfill(parse_args(argv), project_root=Path(__file__).resolve().parents[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
