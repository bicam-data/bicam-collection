#!/usr/bin/env python3
"""Harvest every GovInfo collection in durable, gap-free date windows.

The historical cursor crawls are finite and eventually become terminal.  This
runner owns the ongoing boundary after that backfill: it advances a shared time
frontier only when BILLS, CRPT, CPRT, and CHRG have each completed pagination
and all discovered child fetches without a failed event.  A partial window is
therefore safe to retry after a process, host, or session interruption.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import subprocess
import sys
from uuid import UUID, uuid4

import psycopg

from bicam_collection.config import Settings


LOCK_KEY = "bicam_govinfo_incremental_windows"
RUN_ID_RE = re.compile(r"completed crawl run ([0-9a-f-]{36})")
SECRET_QUERY_RE = re.compile(
    r"([?&](?:api_key|apikey|access_token|token)=)[^&\s]+",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class Collection:
    category: str
    list_node_type: str


COLLECTIONS = (
    Collection("govinfo-bills", "govinfo_bill_package_list"),
    Collection("govinfo-committee-reports", "govinfo_committee_report_package_list"),
    Collection("govinfo-committee-prints", "govinfo_committee_print_package_list"),
    Collection("govinfo-hearings", "govinfo_hearing_package_list"),
)


@dataclass(frozen=True)
class RunEvidence:
    status: str
    list_pages: int
    fetch_events: int
    failed_events: int
    terminal_page_seen: bool


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include a timezone: {value!r}")
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def redact_output(value: str) -> str:
    return SECRET_QUERY_RE.sub(r"\1[REDACTED]", value)


def next_window(
    *,
    frontier: datetime,
    cutoff: datetime,
    window_hours: float,
    minimum_window_minutes: float,
) -> tuple[datetime, datetime] | None:
    if cutoff - frontier < timedelta(minutes=minimum_window_minutes):
        return None
    return frontier, min(frontier + timedelta(hours=window_hours), cutoff)


def build_crawl_command(
    *,
    project_root: Path,
    collection: Collection,
    window_start: datetime,
    window_end: datetime,
    concurrency: int,
    page_size: int,
    request_interval: float,
    key_wait_timeout: float,
) -> list[str]:
    # Deliberately omit --max-pages. A completed ledger row means the list's
    # terminal nextPage was reached, never merely that a page budget expired.
    return [
        sys.executable,
        str(project_root / "scripts" / "run_crawl.py"),
        "--category",
        collection.category,
        "--mode",
        "full",
        "--from-datetime",
        format_timestamp(window_start),
        "--to-datetime",
        format_timestamp(window_end),
        "--page-size",
        str(page_size),
        "--async",
        "--concurrency",
        str(concurrency),
        "--request-interval",
        str(request_interval),
        "--key-wait-timeout",
        str(key_wait_timeout),
    ]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-hours", type=float, default=6.0)
    parser.add_argument("--lag-minutes", type=float, default=5.0)
    parser.add_argument("--minimum-window-minutes", type=float, default=5.0)
    parser.add_argument("--max-windows", type=int, default=4)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--request-interval", type=float, default=0.25)
    parser.add_argument("--key-wait-timeout", type=float, default=600.0)
    parser.add_argument(
        "--until",
        help="Optional inclusive UTC upper boundary; defaults to now minus --lag-minutes.",
    )
    parser.add_argument(
        "--initialize-through",
        help="Create the initial verified historical frontier; refuses when ledger rows already exist.",
    )
    parser.add_argument("--historical-start", default="1789-01-01T00:00:00Z")
    parser.add_argument("--initialize-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    if args.window_hours <= 0 or args.minimum_window_minutes < 0 or args.lag_minutes < 0:
        raise ValueError("window and lag values must be non-negative, with a positive window")
    if args.max_windows < 1 or args.page_size < 1 or args.concurrency < 1:
        raise ValueError("max-windows, page-size, and concurrency must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0:
        raise ValueError("request-interval must be non-negative and key-wait-timeout must be positive")
    if args.initialize_only and not args.initialize_through:
        raise ValueError("--initialize-only requires --initialize-through")


def _initialize_ledger(
    connection: psycopg.Connection,
    *,
    historical_start: datetime,
    initialize_through: datetime,
) -> None:
    existing = connection.execute("select count(*) from govinfo_collection_windows").fetchone()[0]
    if existing:
        raise RuntimeError("GovInfo window ledger is already initialized")
    if initialize_through <= historical_start:
        raise ValueError("initialize-through must be later than historical-start")
    for collection in COLLECTIONS:
        connection.execute(
            """
            insert into govinfo_collection_windows (
                id, category, window_start, window_end, status,
                started_at, finished_at, is_bootstrap
            ) values (%s, %s, %s, %s, 'completed', now(), now(), true)
            """,
            (uuid4(), collection.category, historical_start, initialize_through),
        )
    print(
        "initialized GovInfo historical frontier through "
        f"{format_timestamp(initialize_through)} for {len(COLLECTIONS)} collections",
        flush=True,
    )


def _frontier(connection: psycopg.Connection) -> datetime | None:
    categories = [collection.category for collection in COLLECTIONS]
    row = connection.execute(
        """
        select window_end
        from govinfo_collection_windows
        where status = 'completed'
          and category = any(%s)
        group by window_start, window_end
        having count(distinct category) = %s
        order by window_end desc
        limit 1
        """,
        (categories, len(categories)),
    ).fetchone()
    return row[0].astimezone(timezone.utc) if row else None


def _window_status(
    connection: psycopg.Connection,
    *,
    category: str,
    window_start: datetime,
    window_end: datetime,
) -> str | None:
    row = connection.execute(
        """
        select status
        from govinfo_collection_windows
        where category = %s and window_start = %s and window_end = %s
        """,
        (category, window_start, window_end),
    ).fetchone()
    return row[0] if row else None


def _mark_running(
    connection: psycopg.Connection,
    *,
    category: str,
    window_start: datetime,
    window_end: datetime,
) -> None:
    connection.execute(
        """
        insert into govinfo_collection_windows (
            id, category, window_start, window_end, status, started_at, is_bootstrap
        ) values (%s, %s, %s, %s, 'running', now(), false)
        on conflict (category, window_start, window_end) do update
        set status = 'running', crawl_run_id = null, list_pages = null,
            fetch_events = null, failed_events = null, started_at = now(),
            finished_at = null, error_message = null, is_bootstrap = false
        where govinfo_collection_windows.status <> 'completed'
        """,
        (uuid4(), category, window_start, window_end),
    )


def _mark_completed(
    connection: psycopg.Connection,
    *,
    category: str,
    window_start: datetime,
    window_end: datetime,
    crawl_run_id: UUID,
    evidence: RunEvidence,
) -> None:
    connection.execute(
        """
        update govinfo_collection_windows
        set status = 'completed', crawl_run_id = %s, list_pages = %s,
            fetch_events = %s, failed_events = %s, finished_at = now(),
            error_message = null
        where category = %s and window_start = %s and window_end = %s
        """,
        (
            crawl_run_id,
            evidence.list_pages,
            evidence.fetch_events,
            evidence.failed_events,
            category,
            window_start,
            window_end,
        ),
    )


def _mark_failed(
    connection: psycopg.Connection,
    *,
    category: str,
    window_start: datetime,
    window_end: datetime,
    error_message: str,
    crawl_run_id: UUID | None = None,
    evidence: RunEvidence | None = None,
) -> None:
    connection.execute(
        """
        update govinfo_collection_windows
        set status = 'failed', crawl_run_id = %s, list_pages = %s,
            fetch_events = %s, failed_events = %s, finished_at = now(),
            error_message = %s
        where category = %s and window_start = %s and window_end = %s
        """,
        (
            crawl_run_id,
            evidence.list_pages if evidence else None,
            evidence.fetch_events if evidence else None,
            evidence.failed_events if evidence else None,
            redact_output(error_message)[-2000:],
            category,
            window_start,
            window_end,
        ),
    )


def _run_command(command: list[str], *, project_root: Path) -> UUID:
    process = subprocess.Popen(
        command,
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    output: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        safe_line = redact_output(line.rstrip())
        output.append(safe_line)
        print(safe_line, flush=True)
    return_code = process.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, command, output="\n".join(output))
    match = next((RUN_ID_RE.search(line) for line in reversed(output) if RUN_ID_RE.search(line)), None)
    if match is None:
        raise RuntimeError("crawl completed without emitting a run ID")
    return UUID(match.group(1))


def _run_evidence(
    connection: psycopg.Connection,
    *,
    crawl_run_id: UUID,
    list_node_type: str,
) -> RunEvidence:
    row = connection.execute(
        """
        select
            runs.status,
            count(fetches.id) filter (
                where fetches.node_type = %s and fetches.outcome = 'fetched'
            )::integer as list_pages,
            count(fetches.id)::integer as fetch_events,
            count(fetches.id) filter (where fetches.outcome = 'failed')::integer as failed_events,
            coalesce(bool_or(
                fetches.node_type = %s
                and fetches.outcome = 'fetched'
                and coalesce(versions.response->>'nextPage', '') = ''
            ), false) as terminal_page_seen
        from crawl_runs as runs
        left join crawl_run_fetches as fetches on fetches.crawl_run_id = runs.id
        left join raw_response_versions as versions
          on versions.id = fetches.raw_response_version_id
        where runs.id = %s
        group by runs.status
        """,
        (list_node_type, list_node_type, crawl_run_id),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"crawl run {crawl_run_id} is missing from the durable ledger")
    return RunEvidence(
        status=row[0],
        list_pages=row[1],
        fetch_events=row[2],
        failed_events=row[3],
        terminal_page_seen=row[4],
    )


def _window_is_complete(connection: psycopg.Connection, *, start: datetime, end: datetime) -> bool:
    categories = [collection.category for collection in COLLECTIONS]
    row = connection.execute(
        """
        select count(distinct category)
        from govinfo_collection_windows
        where window_start = %s and window_end = %s
          and status = 'completed' and category = any(%s)
        """,
        (start, end, categories),
    ).fetchone()
    return row[0] == len(categories)


def _run_window(
    connection: psycopg.Connection,
    *,
    project_root: Path,
    args: argparse.Namespace,
    window_start: datetime,
    window_end: datetime,
) -> bool:
    print(
        f"starting GovInfo window {format_timestamp(window_start)} -> {format_timestamp(window_end)}",
        flush=True,
    )
    for collection in COLLECTIONS:
        status = _window_status(
            connection,
            category=collection.category,
            window_start=window_start,
            window_end=window_end,
        )
        if status == "completed":
            print(f"{collection.category}: already completed; skipping", flush=True)
            continue
        command = build_crawl_command(
            project_root=project_root,
            collection=collection,
            window_start=window_start,
            window_end=window_end,
            concurrency=args.concurrency,
            page_size=args.page_size,
            request_interval=args.request_interval,
            key_wait_timeout=args.key_wait_timeout,
        )
        if args.dry_run:
            print(" ".join(command), flush=True)
            continue
        _mark_running(
            connection,
            category=collection.category,
            window_start=window_start,
            window_end=window_end,
        )
        try:
            run_id = _run_command(command, project_root=project_root)
            evidence = _run_evidence(
                connection,
                crawl_run_id=run_id,
                list_node_type=collection.list_node_type,
            )
            if evidence.status != "completed":
                raise RuntimeError(f"crawl run {run_id} finished with status {evidence.status!r}")
            if not evidence.terminal_page_seen:
                raise RuntimeError(f"crawl run {run_id} did not persist a terminal list page")
            if evidence.failed_events:
                raise RuntimeError(
                    f"crawl run {run_id} retained {evidence.failed_events} failed fetch event(s)"
                )
            _mark_completed(
                connection,
                category=collection.category,
                window_start=window_start,
                window_end=window_end,
                crawl_run_id=run_id,
                evidence=evidence,
            )
            print(
                f"{collection.category}: completed run={run_id} "
                f"list_pages={evidence.list_pages} fetch_events={evidence.fetch_events}",
                flush=True,
            )
        except Exception as exc:
            run_id = locals().get("run_id")
            evidence = locals().get("evidence")
            _mark_failed(
                connection,
                category=collection.category,
                window_start=window_start,
                window_end=window_end,
                error_message=str(exc),
                crawl_run_id=run_id if isinstance(run_id, UUID) else None,
                evidence=evidence if isinstance(evidence, RunEvidence) else None,
            )
            print(f"{collection.category}: failed: {redact_output(str(exc))}", file=sys.stderr, flush=True)
        finally:
            # Avoid carrying one category's locals into the next exception.
            run_id = None
            evidence = None
    if args.dry_run:
        return True
    return _window_is_complete(connection, start=window_start, end=window_end)


def _cutoff(args: argparse.Namespace) -> datetime:
    if args.until:
        return parse_timestamp(args.until)
    return (datetime.now(timezone.utc) - timedelta(minutes=args.lag_minutes)).replace(microsecond=0)


def run(argv: list[str] | None = None, *, project_root: Path | None = None) -> int:
    root = (project_root or Path(__file__).resolve().parents[1]).resolve()
    args = parse_args(argv)
    validate_args(args)
    settings = Settings.from_env(root)
    with psycopg.connect(settings.database_url, autocommit=True) as connection:
        acquired = connection.execute(
            "select pg_try_advisory_lock(hashtext(%s))", (LOCK_KEY,)
        ).fetchone()[0]
        if not acquired:
            print("another GovInfo incremental-window runner owns the durable lock; skipping", flush=True)
            return 0
        try:
            if args.initialize_through:
                if args.dry_run:
                    print(
                        "would initialize GovInfo historical frontier through "
                        f"{format_timestamp(parse_timestamp(args.initialize_through))}",
                        flush=True,
                    )
                else:
                    _initialize_ledger(
                        connection,
                        historical_start=parse_timestamp(args.historical_start),
                        initialize_through=parse_timestamp(args.initialize_through),
                    )
            if args.initialize_only:
                return 0

            frontier = _frontier(connection)
            if frontier is None:
                raise RuntimeError(
                    "GovInfo window ledger is uninitialized; verify historical coverage and use "
                    "--initialize-through once"
                )
            cutoff = _cutoff(args)
            completed_windows = 0
            for _ in range(args.max_windows):
                window = next_window(
                    frontier=frontier,
                    cutoff=cutoff,
                    window_hours=args.window_hours,
                    minimum_window_minutes=args.minimum_window_minutes,
                )
                if window is None:
                    print(
                        f"GovInfo frontier {format_timestamp(frontier)} is current through the "
                        f"cutoff {format_timestamp(cutoff)}",
                        flush=True,
                    )
                    break
                window_start, window_end = window
                if not _run_window(
                    connection,
                    project_root=root,
                    args=args,
                    window_start=window_start,
                    window_end=window_end,
                ):
                    print("GovInfo frontier held because the window is incomplete", file=sys.stderr, flush=True)
                    return 1
                if args.dry_run:
                    break
                frontier = window_end
                completed_windows += 1
            print(
                f"GovInfo incremental windows completed={completed_windows} "
                f"frontier={format_timestamp(frontier)}",
                flush=True,
            )
            return 0
        finally:
            connection.execute("select pg_advisory_unlock(hashtext(%s))", (LOCK_KEY,))


if __name__ == "__main__":
    raise SystemExit(run())
