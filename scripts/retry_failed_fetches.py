"""Retry a bounded set of failed raw fetches without replaying partitions.

The normal historical runner is intentionally partition-oriented.  This helper
is for the smaller residual set of failed child/detail URLs left by rate limits
or transient source errors.  It preserves the original node type so a direct
detail retry is not accidentally stored as a list payload.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import load_spec, resolve_spec_path
from bicam_collection.storage import AsyncPostgresStore
from bicam_collection.target_scope import TARGET_CONGRESS_CATEGORIES


@dataclass(frozen=True, slots=True)
class FailedFetch:
    category: str
    endpoint_url: str
    node_type: str
    failed_at: object
    http_status: int | None = None
    error: str | None = None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retry a bounded set of unresolved failed crawler URLs.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum failed URLs to retry in this batch.")
    parser.add_argument(
        "--categories",
        nargs="+",
        default=list(TARGET_CONGRESS_CATEGORIES),
        help=(
            "Restrict retries to these spec categories (defaults to the "
            "authoritative Congress.gov main-11 scope)."
        ),
    )
    parser.add_argument(
        "--endpoint",
        help="Retry only this exact endpoint URL (useful for a bounded single-page repair).",
    )
    parser.add_argument(
        "--node-types",
        nargs="+",
        default=None,
        help="Restrict retries to these crawler node types (for example, bill_detail amendment_detail).",
    )
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument("--request-interval", type=float, default=5.0)
    parser.add_argument("--key-wait-timeout", type=float, default=300.0)
    parser.add_argument(
        "--min-age-minutes",
        type=float,
        default=15.0,
        help="Only retry failures older than this many minutes (default: 15).",
    )
    parser.add_argument(
        "--include-source-500",
        action="store_true",
        help="Include HTTP 500 failures; omitted by default because known malformed source URLs recur.",
    )
    parser.add_argument(
        "--only-source-500",
        action="store_true",
        help="When retrying source 500s, exclude all other transient status codes from this batch.",
    )
    parser.add_argument("--dry-run", action="store_true", help="List candidates without making requests.")
    parser.add_argument(
        "--no-descendants",
        action="store_true",
        help="Retry the seed pagination stream without following child resources.",
    )
    return parser.parse_args(argv)


def _without_descendants(spec):
    """Return an isolated spec that cannot fan out from a retry seed.

    A failed list page is often a useful retry target by itself, but a normal
    ``backfill_descendants`` crawl follows every child edge in its payload.
    Copying the spec keeps the normal crawl behavior unchanged while allowing
    bounded retries to suppress that fan-out safely.
    """

    retry_spec = copy.deepcopy(spec)
    for node in retry_spec.nodes.values():
        node.child_edges = []
        node.linked_item_edges = []
    return retry_spec


def load_failed_fetches(
    *,
    database_url: str,
    limit: int,
    categories: list[str] | None,
    endpoint_url: str | None = None,
    node_types: list[str] | None = None,
    min_age_minutes: float = 15.0,
    include_source_500: bool = False,
    only_source_500: bool = False,
) -> list[FailedFetch]:
    category_filter = ""
    failed_endpoint_filter = ""
    node_type_filter = ""
    retryable_statuses = [408, 425, 429, 502, 503, 504, 520, 522, 523, 524]
    if only_source_500:
        include_source_500 = True
        retryable_statuses = [500]
        failed_status_filter = "failed.http_status = 500"
        source_500_filter = ""
    elif include_source_500:
        retryable_statuses.append(500)
        failed_status_filter = "(failed.http_status = any(%s) or failed.http_status is null)"
        source_500_filter = ""
    else:
        # A URL that has already produced a source-side 500 is retained in the
        # ledger, but a later 429 from a retry must not make it look transient.
        # Operators can explicitly opt back in with --include-source-500.
        source_500_filter = """
              and not exists (
                  select 1
                  from failed_requests as prior_500
                  where prior_500.endpoint_url = failed.endpoint_url
                    and prior_500.node_type = failed.node_type
                    and prior_500.http_status = 500
              )
        """
        failed_status_filter = "(failed.http_status = any(%s) or failed.http_status is null)"
    # ``--include-source-500`` is an explicit operator request to revisit
    # deterministic source failures (normally excluded to avoid replaying the
    # same malformed URL forever).  Keep the known-error guard for ordinary
    # retry passes, but do not let it silently override the opt-in flag.
    known_source_error_filter = "" if include_source_500 else """
              and not exists (
                  select 1
                  from known_source_errors as known
                  where known.category = failed.category
                    and known.endpoint_url = failed.endpoint_url
                    and known.node_type = failed.node_type
              )
        """
    query_params: list[object] = []
    if not only_source_500:
        query_params.append(retryable_statuses)
    query_params.append(min_age_minutes)
    if categories:
        # Retry candidates come from failed_requests, which is the compact
        # append-only failure ledger.  Keep the filter on that relation so a
        # retry pass does not scan the multi-gigabyte crawl_run_fetches table.
        category_filter = "and failed.category = any(%s)"
        query_params.append(categories)
    if endpoint_url:
        failed_endpoint_filter = "and failed.endpoint_url = %s"
        query_params.append(endpoint_url)
    if node_types:
        node_type_filter = "and failed.node_type = any(%s)"
        query_params.append(node_types)
    query_params.append(max(1, limit))
    # An explicit endpoint request is an operator-directed repair, so it must
    # be allowed through even when a broader successful page partition would
    # normally suppress retries for a deterministic source error.  Automatic
    # retry sweeps keep the coverage suppression below to avoid quota churn.
    covered_partition_filter = "" if endpoint_url else """
              and not (
                  right(failed.node_type, 5) = '_list'
                  and exists (
                      select 1
                      from successful_list_partitions as covered
                      where covered.category = failed.category
                        and covered.node_type = failed.node_type
                        and covered.endpoint_path = case
                            when failed.category = 'hearings' then regexp_replace(
                                split_part(failed.endpoint_url, '?', 1),
                                '/(house|senate|joint|nochamber)$',
                                ''
                            )
                            else split_part(failed.endpoint_url, '?', 1)
                        end
                  )
              )
    """
    query = f"""
        with
        known_source_errors as (
            -- The client records a compact source_error code for deterministic
            -- HTTP 500 payloads.  Keep those URLs visible in the audit, but do
            -- not spend quota replaying the same upstream data defect on every
            -- supervisor pass.
            select distinct category, endpoint_url, node_type
            from failed_requests
            -- psycopg uses percent-style placeholders, so literal LIKE wildcards
            -- must be escaped as ``%%`` before the query is executed.
            where error ilike '%%source_error=%%'
        ),
        successful_list_partitions as materialized (
            -- Older crawls experimented with smaller page sizes and
            -- chamber-qualified hearing URLs after source 500s. Once the
            -- successful pages for the enclosing partition enumerate at least
            -- the source-reported item count, those failed URL spellings are
            -- not missing data and must not consume quota forever.
            select
                responses.category,
                responses.node_type,
                case
                    when responses.category = 'hearings' then regexp_replace(
                        split_part(responses.endpoint_url, '?', 1),
                        '/(house|senate|joint|nochamber)$',
                        ''
                    )
                    else split_part(responses.endpoint_url, '?', 1)
                end as endpoint_path,
                max((responses.response -> 'pagination' ->> 'count')::bigint) as expected_item_count,
                sum(coalesce((
                    select max(jsonb_array_length(item.value))
                    from jsonb_each(responses.response) as item(key, value)
                    where jsonb_typeof(item.value) = 'array'
                ), 0))::bigint as enumerated_item_count
            from raw_responses as responses
            where right(responses.node_type, 5) = '_list'
              and responses.response ? 'pagination'
            group by 1, 2, 3
            having sum(coalesce((
                select max(jsonb_array_length(item.value))
                from jsonb_each(responses.response) as item(key, value)
                where jsonb_typeof(item.value) = 'array'
            ), 0)) >= max((responses.response -> 'pagination' ->> 'count')::bigint)
        ),
        latest_failed_requests as (
            -- failed_requests is append-only, so apply the same latest-row
            -- rule there before filtering to transient statuses.
            select distinct on (failed.category, failed.endpoint_url, failed.node_type)
                failed.category,
                failed.endpoint_url,
                failed.node_type,
                failed.http_status,
                failed.error,
                failed.failed_at
            from failed_requests as failed
            order by failed.category, failed.endpoint_url, failed.node_type, failed.failed_at desc
        ),
        failed_request_candidates as (
            select
                failed.category,
                failed.endpoint_url,
                failed.node_type,
                failed.failed_at as fetched_at,
                failed.http_status,
                failed.error
            from latest_failed_requests as failed
            where {failed_status_filter}
              and failed.failed_at < now() - (%s * interval '1 minute')
              {category_filter}
              {failed_endpoint_filter}
              {node_type_filter}
              {source_500_filter}
              {known_source_error_filter}
              and not exists (
                  select 1
                  from raw_responses as responses
                  where responses.endpoint_url = failed.endpoint_url
              )
              {covered_partition_filter}
        ),
        candidates as (
            -- failed_requests is already reduced to the latest failure per
            -- URL/node above; keep this named relation for a stable result
            -- shape and a bounded LIMIT.
            select category, endpoint_url, node_type, fetched_at, http_status, error
            from failed_request_candidates
        )
        select category, endpoint_url, node_type, fetched_at, http_status, error
        from candidates
        order by fetched_at asc
        limit %s
    """
    with psycopg.connect(database_url) as connection:
        # Failure selection scans a durable telemetry ledger that grows with
        # the crawl.  Keep a cancelled or degraded retry pass from leaving a
        # long-running PostgreSQL backend behind the supervisor timeout.
        connection.execute("set statement_timeout='120s'", ())
        # Candidate selection intentionally uses only the compact failure
        # ledger; the large fetch ledger is consulted later for this retry
        # run's outcome summary.
        rows = connection.execute(query, query_params).fetchall()
    return [
        FailedFetch(
            category=row[0],
            endpoint_url=row[1],
            node_type=row[2],
            failed_at=row[3],
            http_status=row[4],
            error=row[5],
        )
        for row in rows
    ]


def summarize_retry_run(*, database_url: str, crawl_run_id: str) -> tuple[int, int]:
    """Return ``(failed, rate_limited)`` for one bounded retry run.

    A retry batch must stop after the first HTTP 429.  Continuing to spend the
    remaining candidates while the source-wide window is exhausted only
    creates more durable failures and can cool every configured key.
    """

    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """
            select count(*), count(*) filter (where http_status = 429)
            from crawl_run_fetches
            where crawl_run_id = %s
              and outcome = 'failed'
            """,
            (crawl_run_id,),
        ).fetchone()
    if not row:
        return 0, 0
    return int(row[0] or 0), int(row[1] or 0)


def _seed_path(endpoint_url: str, source: str, *, page_size: int | None = None) -> str:
    parsed = urlparse(endpoint_url)
    prefix = "/v3" if source == "congress" else ""
    path = parsed.path
    if prefix and path.startswith(prefix):
        path = path[len(prefix) :]
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if page_size is not None:
        # Failed list pages can be source-side 500s at a large limit while the
        # same offset succeeds at a smaller page size.  Replace the existing
        # value; ``with_query_params`` intentionally preserves existing keys.
        if "limit" in query:
            query["limit"] = str(page_size)
        if "pageSize" in query:
            query["pageSize"] = str(page_size)
    return urlunparse(("", "", path, "", urlencode(query), ""))


def _retry_page_size(candidate: FailedFetch, requested: int) -> int:
    """Choose a conservative page size for source-side HTTP 500 retries.

    Congress.gov and GovInfo occasionally return a 500 for a large list page
    even though a smaller page at the same offset is available.  Source-500
    recovery is deliberately bounded to ten rows.  If the failed URL already
    used an even smaller page (as the malformed cosponsor pages do), preserve
    that size so a retry cannot broaden the malformed response and create a
    new failure.
    """

    if candidate.http_status != 500:
        return requested
    parsed = urlparse(candidate.endpoint_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    existing_sizes: list[int] = []
    for key in ("limit", "pageSize"):
        value = query.get(key)
        if value is None:
            continue
        try:
            parsed_size = int(value)
        except (TypeError, ValueError):
            continue
        if parsed_size > 0:
            existing_sizes.append(parsed_size)
    retry_size = min(requested, 10)
    if existing_sizes:
        retry_size = min(retry_size, min(existing_sizes))
    return retry_size


async def run_retries(args: argparse.Namespace, *, project_root: Path) -> tuple[int, int]:
    settings = Settings.from_env(project_root)
    candidates = load_failed_fetches(
        database_url=settings.database_url,
        limit=args.limit,
        categories=args.categories,
        endpoint_url=args.endpoint,
        node_types=args.node_types,
        min_age_minutes=args.min_age_minutes,
        include_source_500=args.include_source_500,
        only_source_500=args.only_source_500,
    )
    if args.dry_run:
        for candidate in candidates:
            print(f"{candidate.category}\t{candidate.node_type}\t{candidate.endpoint_url}")
        return len(candidates), 0
    if not candidates:
        print("no unresolved failed fetches need retry")
        return 0, 0

    specs = {}
    clients = {}
    for candidate in candidates:
        if candidate.category not in specs:
            spec = load_spec(resolve_spec_path(project_root / "specs", candidate.category))
            specs[candidate.category] = spec
            source = spec.source
            if source not in clients:
                clients[source] = build_client(
                    spec=spec,
                    env_path=settings.env_path,
                    use_async=True,
                    key_wait_timeout=args.key_wait_timeout,
                    request_interval=args.request_interval,
                )

    store = AsyncPostgresStore(settings.database_url)
    attempted = 0
    failed = 0
    try:
        for candidate in candidates:
            spec = specs[candidate.category]
            if candidate.node_type not in spec.nodes:
                print(f"[retry] skipped unknown node type {candidate.category}/{candidate.node_type}", flush=True)
                failed += 1
                continue
            crawler_spec = _without_descendants(spec) if args.no_descendants else spec
            retry_page_size = _retry_page_size(candidate, args.page_size)
            crawler = AsyncSpecCrawler(
                base_url=base_url_for_spec(settings, spec),
                spec=crawler_spec,
                client=clients[spec.source],
                store=store,
                concurrency=1,
            )
            try:
                run_id = await crawler.run(
                    CrawlOptions(
                        mode="retry_no_descendants" if args.no_descendants else "backfill_descendants",
                        seed_path=_seed_path(candidate.endpoint_url, spec.source, page_size=retry_page_size),
                        seed_node_type=candidate.node_type,
                    # ``--no-descendants`` removes child edges but should not
                    # suppress pagination: a source-side 500 on a 250-row
                    # list page is repaired by retrying the same offset with a
                    # smaller page size across the bounded page sequence.
                    max_pages=args.max_pages,
                        page_size=retry_page_size,
                    )
                )
                attempted += 1
                run_failures, rate_limited = summarize_retry_run(
                    database_url=settings.database_url,
                    crawl_run_id=run_id,
                )
                if run_failures:
                    failed += 1
                    print(
                        f"[retry] completed with {run_failures} failed fetch(es) "
                        f"{candidate.category}/{candidate.node_type} {candidate.endpoint_url}",
                        flush=True,
                    )
                else:
                    print(f"[retry] completed {candidate.category}/{candidate.node_type} {candidate.endpoint_url}", flush=True)
                if rate_limited:
                    print(
                        "[retry] stopping batch after HTTP 429; the source quota window is exhausted",
                        flush=True,
                    )
                    break
            except Exception as exc:
                failed += 1
                print(f"[retry] failed {candidate.category}/{candidate.node_type} {candidate.endpoint_url}: {exc}", flush=True)
    finally:
        await store.close()
    print(f"[retry] attempted={attempted} failed={failed}", flush=True)
    return attempted, failed


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit < 1 or args.max_pages < 1 or args.page_size < 1:
        raise SystemExit("limit, max-pages, and page-size must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0 or args.min_age_minutes < 0:
        raise SystemExit("request-interval and min-age-minutes must be non-negative; key-wait-timeout must be positive")
    asyncio.run(run_retries(args, project_root=Path(__file__).resolve().parents[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
