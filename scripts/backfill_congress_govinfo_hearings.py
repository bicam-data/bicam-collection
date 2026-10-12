"""Fetch Congress.gov hearing details for modern GovInfo-only hearing packages.

``CHRG-119hhrg63835`` maps to ``/hearing/119/house/63835``; joint hearings use the
legacy ``/nochamber/`` route.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path
import re

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
    category="hearings",
    node_type="hearing_detail",
    endpoint_name="congress-hearing-from-govinfo-package",
    endpoint_priority=3,
    run_mode="targeted_govinfo_hearing_backfill",
    record_failed_fetches=True,
)
_PACKAGE_RE = re.compile(r"^CHRG-(?P<congress>[0-9]+)(?P<kind>[hsj]hrg)(?P<number>[0-9]+).*$", re.I)
_ROUTE_CHAMBERS = {"hhrg": "house", "shrg": "senate", "jhrg": "nochamber"}


@dataclass(frozen=True, slots=True)
class GovInfoHearing:
    package_id: str
    parent_raw_id: str
    parent_endpoint_url: str
    congress: int
    route_chamber: str
    jacket_number: int

    @property
    def endpoint_url(self) -> str:
        return (
            f"https://api.congress.gov/v3/hearing/{self.congress}/"
            f"{self.route_chamber}/{self.jacket_number}?format=json"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch modern Congress.gov hearings missing from GovInfo linkage."
    )
    parser.add_argument("--congress-start", type=int, default=104)
    parser.add_argument("--congress-end", type=int, default=119)
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--request-interval", type=float, default=1.0)
    parser.add_argument("--key-wait-timeout", type=float, default=600.0)
    parser.add_argument("--active-key-budget", type=int, default=2)
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args(argv)


def _parse_package(package_id: str) -> tuple[int, str, int] | None:
    match = _PACKAGE_RE.match(package_id)
    if not match:
        return None
    kind = match.group("kind").lower()
    route_chamber = _ROUTE_CHAMBERS.get(kind)
    if route_chamber is None:
        return None
    return int(match.group("congress")), route_chamber, int(match.group("number"))


def load_missing(
    *, database_url: str, congress_start: int, congress_end: int, limit: int, refresh: bool = False
) -> list[GovInfoHearing]:
    existing_clause = "" if refresh else """
        and not exists (
            select 1
            from raw_responses as existing
            where existing.source = 'congress'
              and existing.category = 'hearings'
              and existing.node_type = 'hearing_detail'
              and existing.endpoint_url in (expected.endpoint_url, expected.endpoint_path)
        )
        and not exists (
            select 1
            from failed_requests as prior_failure
            where prior_failure.endpoint_url in (expected.endpoint_url, expected.endpoint_path)
              and prior_failure.category = 'hearings'
              and prior_failure.node_type = 'hearing_detail'
              and prior_failure.http_status = 404
        )
    """
    query = f"""
        with packages as (
            select distinct on (gov.package_id)
                gov.package_id,
                gov.raw_id::text as parent_raw_id,
                gov.endpoint_url as parent_endpoint_url,
                gov.congress,
                lower(gov.chamber) as source_chamber,
                (regexp_match(gov.package_id, '^CHRG-([0-9]+)([hsj]hrg)([0-9]+).*$'))[2] as kind,
                ((regexp_match(gov.package_id, '^CHRG-([0-9]+)([hsj]hrg)([0-9]+).*$'))[3])::integer as jacket_number
            from stg_govinfo__hearings_packages as gov
            where gov.congress between %s and %s
              and gov.package_id ~* '^CHRG-[0-9]+[hsj]hrg[0-9]+.*$'
            order by gov.package_id, gov.last_modified desc nulls last
        ), expected as (
            select
                packages.*,
                case packages.kind
                    when 'hhrg' then 'house'
                    when 'shrg' then 'senate'
                    when 'jhrg' then 'nochamber'
                end as route_chamber,
                'https://api.congress.gov/v3/hearing/'
                    || packages.congress::text || '/'
                    || case packages.kind
                        when 'hhrg' then 'house'
                        when 'shrg' then 'senate'
                        when 'jhrg' then 'nochamber'
                       end
                    || '/' || packages.jacket_number::text || '?format=json' as endpoint_url,
                regexp_replace(
                    'https://api.congress.gov/v3/hearing/'
                    || packages.congress::text || '/'
                    || case packages.kind
                        when 'hhrg' then 'house'
                        when 'shrg' then 'senate'
                        when 'jhrg' then 'nochamber'
                       end
                    || '/' || packages.jacket_number::text || '?format=json',
                    '[?].*$', ''
                ) as endpoint_path
            from packages
        )
        select package_id,parent_raw_id,parent_endpoint_url,congress,route_chamber,jacket_number
        from expected
        where route_chamber is not null
          and jacket_number is not null
          and not exists (
              select 1
              from int_hearing_committee_meeting_links as links
              where links.relationship_type = 'govinfo_hearing_to_congress_hearing'
                and links.source_id = expected.package_id
          )
          {existing_clause}
        order by congress, route_chamber, jacket_number
        limit %s
    """
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(
            query, (congress_start, congress_end, max(1, limit))
        ).fetchall()
    return [
        GovInfoHearing(
            package_id=str(row[0]),
            parent_raw_id=str(row[1]),
            parent_endpoint_url=str(row[2]),
            congress=int(row[3]),
            route_chamber=str(row[4]),
            jacket_number=int(row[5]),
        )
        for row in rows
    ]


async def run(args: argparse.Namespace, *, project_root: Path) -> tuple[int, int]:
    settings = Settings.from_env(project_root)
    hearings = load_missing(
        database_url=settings.database_url,
        congress_start=args.congress_start,
        congress_end=args.congress_end,
        limit=args.limit,
        refresh=args.refresh,
    )
    print(f"[govinfo-hearing] candidates={len(hearings)}", flush=True)
    if not hearings:
        return 0, 0

    spec = load_spec(project_root / "specs/hearings.yaml")
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=max(0.0, args.request_interval),
        active_key_budget=max(1, args.active_key_budget),
    )
    store = AsyncPostgresStore(settings.database_url)
    candidates = [
        TargetedCandidate(
            endpoint_url=canonicalize_congress_api_url(hearing.endpoint_url),
            parent_id=hearing.parent_raw_id,
            parent_endpoint_url=hearing.parent_endpoint_url,
        )
        for hearing in hearings
    ]
    outcome = await run_targeted_backfill(
        FAMILY,
        candidates,
        client=client,
        store=store,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
    )
    print(
        f"[govinfo-hearing] completed run={outcome.run_id} "
        f"fetched={outcome.fetched} failed={outcome.failed}",
        flush=True,
    )
    return outcome.fetched, outcome.failed


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.congress_start < 1 or args.congress_end < args.congress_start:
        raise SystemExit("congress range must be valid")
    if args.limit < 1 or args.concurrency < 1 or args.batch_size < 1 or args.active_key_budget < 1:
        raise SystemExit("limit, concurrency, batch-size, and active-key-budget must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0:
        raise SystemExit("request-interval must be non-negative and key-wait-timeout must be positive")
    asyncio.run(run(args, project_root=Path(__file__).resolve().parents[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
