"""Fetch documented law-detail routes derived from stored law-list rows.

Congress.gov's ``/law/{congress}`` list returns bill-shaped rows whose ``url``
points to ``/bill/...``.  The documented law detail endpoint instead uses the
embedded public/private law number.  This targeted repair constructs those
routes explicitly so the crawler does not misclassify ordinary bill details as
law endpoint responses.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

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
    category="laws",
    node_type="law_detail",
    endpoint_name="congress-laws-root-detail",
    endpoint_priority=4,
    run_mode="targeted_law_detail_backfill",
    record_failed_fetches=True,
    fail_run_on_any_failure=True,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--congress-start", type=int, default=1)
    parser.add_argument("--congress-end", type=int, default=119)
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--request-interval", type=float, default=0.5)
    parser.add_argument("--key-wait-timeout", type=float, default=1200.0)
    parser.add_argument("--active-key-budget", type=int, default=4)
    parser.add_argument("--max-source-error-attempts", type=int, default=3)
    return parser.parse_args(argv)


def law_type_code(value: str | None) -> str | None:
    normalized = " ".join((value or "").strip().lower().split())
    if normalized in {"public law", "public", "pub"}:
        return "pub"
    if normalized in {"private law", "private", "priv"}:
        return "priv"
    return None


def law_number_component(value: str | int | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    component = raw.rsplit("-", 1)[-1]
    return component if component.isdigit() else None


def law_detail_url(*, congress: int, law_type: str | None, law_number: str | int | None) -> str | None:
    type_code = law_type_code(law_type)
    number = law_number_component(law_number)
    if congress < 1 or type_code is None or number is None:
        return None
    return canonicalize_congress_api_url(
        f"https://api.congress.gov/v3/law/{congress}/{type_code}/{number}?format=json"
    )


def load_missing(
    *,
    database_url: str,
    congress_start: int,
    congress_end: int,
    limit: int,
    max_source_error_attempts: int,
) -> list[TargetedCandidate]:
    query = """
        with candidates as (
            select distinct on (detail_url)
                detail_url,
                parent_raw_id,
                parent_endpoint_url
            from (
                select
                    'https://api.congress.gov/v3/law/'
                    || (bill->>'congress') || '/'
                    || case lower(law->>'type')
                        when 'public law' then 'pub'
                        when 'private law' then 'priv'
                    end || '/'
                    || substring(law->>'number' from '([0-9]+)$')
                    || '?format=json' as detail_url,
                    lists.id::text as parent_raw_id,
                    lists.endpoint_url as parent_endpoint_url,
                    lists.fetched_at
                from raw_responses as lists
                cross join lateral jsonb_array_elements(
                    coalesce(lists.response->'bills', '[]'::jsonb)
                ) as bill
                cross join lateral jsonb_array_elements(
                    coalesce(bill->'laws', '[]'::jsonb)
                ) as law
                where lists.source = 'congress'
                  and lists.category = 'laws'
                  and lists.node_type = 'law_list'
                  and (bill->>'congress')::integer between %s and %s
                  and lower(law->>'type') in ('public law', 'private law')
                  and substring(law->>'number' from '([0-9]+)$') is not null
            ) as derived
            order by detail_url, fetched_at desc
        ), source_failures as (
            select endpoint_url, count(*) as attempts
            from failed_requests
            where category = 'laws'
              and node_type = 'law_detail'
              and http_status in (404, 500)
            group by endpoint_url
        )
        select candidates.detail_url, candidates.parent_raw_id, candidates.parent_endpoint_url
        from candidates
        left join source_failures on source_failures.endpoint_url = candidates.detail_url
        where not exists (
            select 1
            from raw_responses as detail
            where detail.endpoint_url = candidates.detail_url
        )
          and coalesce(source_failures.attempts, 0) < %s
        order by candidates.detail_url
        limit %s
    """
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(
            query,
            (
                congress_start,
                congress_end,
                max(1, max_source_error_attempts),
                max(1, limit),
            ),
        ).fetchall()
    return [
        TargetedCandidate(
            endpoint_url=canonicalize_congress_api_url(str(row[0])),
            parent_id=str(row[1]),
            parent_endpoint_url=str(row[2]),
        )
        for row in rows
    ]


async def run(args: argparse.Namespace) -> int:
    if args.congress_start < 1 or args.congress_end < args.congress_start:
        raise ValueError("Congress range must satisfy 1 <= start <= end")
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    details = load_missing(
        database_url=settings.database_url,
        congress_start=args.congress_start,
        congress_end=args.congress_end,
        limit=args.limit,
        max_source_error_attempts=args.max_source_error_attempts,
    )
    print(f"[law-detail] candidates={len(details)}", flush=True)
    if not details:
        return 0

    spec = load_spec(project_root / "specs" / "laws.yaml")
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=max(0.0, args.request_interval),
        active_key_budget=max(1, args.active_key_budget),
    )
    store = AsyncPostgresStore(settings.database_url)
    outcome = await run_targeted_backfill(
        FAMILY,
        details,
        client=client,
        store=store,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
    )
    print(
        f"[law-detail] completed run={outcome.run_id} "
        f"fetched={outcome.fetched} failed={outcome.failed}",
        flush=True,
    )
    return 1 if outcome.failed else 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    positive = (
        args.limit,
        args.concurrency,
        args.batch_size,
        args.active_key_budget,
        args.max_source_error_attempts,
    )
    if any(value < 1 for value in positive):
        raise ValueError("limit, concurrency, batch size, key budget, and attempts must be positive")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
