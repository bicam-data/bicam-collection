#!/usr/bin/env python3
"""Run bounded GovInfo cursor windows and advance them from stored nextPage URLs.

The GovInfo collection endpoints expose opaque cursor URLs.  This runner keeps
each crawl bounded, persists every window through ``run_crawl.py``, and obtains
the next cursor from the completed run's raw list response instead of guessing
or reconstructing offset tokens.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
import subprocess
import sys

import psycopg

from bicam_collection.config import Settings


RUN_ID_RE = re.compile(r"completed crawl run ([0-9a-f-]{36})")
SECRET_QUERY_RE = re.compile(
    r"([?&](?:api_key|apikey|access_token|token)=)[^&\s]+",
    flags=re.IGNORECASE,
)


def redact_output(value: str) -> str:
    """Remove credentials if a client exception includes a prepared URL."""

    return SECRET_QUERY_RE.sub(r"\1[REDACTED]", value)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--category", required=True)
    parser.add_argument("--seed-path")
    parser.add_argument(
        "--resume-latest-completed",
        action="store_true",
        help=(
            "Resume from the newest list response fetched by a completed crawl; "
            "exit successfully when that response is terminal."
        ),
    )
    parser.add_argument("--seed-node-type", required=True)
    parser.add_argument("--batches", type=int, default=1, help="Maximum cursor windows to run.")
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--page-size", type=int, default=100)
    # Three GovInfo credentials already cap useful in-flight work at three
    # requests. The 0.25-second process-wide pacer keeps future bounded
    # windows below a conservative aggregate envelope while using the key
    # pool more fully. Operators can lower these values for a source incident
    # without changing cursor semantics.
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--request-interval", type=float, default=0.25)
    parser.add_argument("--key-wait-timeout", type=float, default=60.0)
    return parser.parse_args(argv)


def _ensure_positive(args: argparse.Namespace) -> None:
    if not args.resume_latest_completed and not args.seed_path:
        raise ValueError("seed-path is required unless --resume-latest-completed is used")
    if args.batches < 1:
        raise ValueError("batches must be positive")
    if args.max_pages < 1 or args.page_size < 1 or args.concurrency < 1:
        raise ValueError("max-pages, page-size, and concurrency must be positive")
    if args.request_interval < 0 or args.key_wait_timeout <= 0:
        raise ValueError("request-interval must be non-negative and key-wait-timeout must be positive")


def _assert_category_idle(database_url: str, category: str) -> None:
    with psycopg.connect(database_url) as connection:
        running = connection.execute(
            "select 1 from crawl_runs where category = %s and status = 'running' limit 1",
            (category,),
        ).fetchone()
    if running:
        raise RuntimeError(f"a crawl is already running for category {category!r}")


def _run_one_batch(*, root: Path, args: argparse.Namespace, seed_path: str) -> str:
    command = [
        sys.executable,
        str(root / "scripts" / "run_crawl.py"),
        "--category",
        args.category,
        "--mode",
        "full",
        "--seed-path",
        seed_path,
        "--seed-node-type",
        args.seed_node_type,
        "--max-pages",
        str(args.max_pages),
        "--page-size",
        str(args.page_size),
        "--async",
        "--concurrency",
        str(args.concurrency),
        "--key-wait-timeout",
        str(args.key_wait_timeout),
        "--request-interval",
        str(args.request_interval),
    ]
    process = subprocess.Popen(
        command,
        cwd=root,
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
    return match.group(1)


def _next_page(database_url: str, *, run_id: str, node_type: str) -> str | None:
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """
            select versions.response->>'nextPage'
            from crawl_run_fetches as fetches
            join raw_response_versions as versions
              on versions.id = fetches.raw_response_version_id
            where fetches.crawl_run_id = %s
              and fetches.node_type = %s
              and fetches.outcome = 'fetched'
            order by fetches.fetched_at desc, fetches.id desc
            limit 1
            """,
            (run_id, node_type),
        ).fetchone()
    return row[0] if row and row[0] else None


def _latest_completed_seed(
    database_url: str, *, category: str, node_type: str
) -> tuple[bool, str | None]:
    """Return whether a durable cursor exists and its next page, if any.

    A completed terminal response is meaningful state.  Returning
    ``(True, None)`` prevents the scheduler from skipping that marker and
    replaying the previous nonterminal page forever.  Responses from running,
    failed, or abandoned crawls are ignored so an interrupted batch cannot
    advance past children that were not durably completed.
    """

    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """
            select versions.response->>'nextPage'
            from crawl_run_fetches as fetches
            join raw_response_versions as versions
              on versions.id = fetches.raw_response_version_id
            join crawl_runs as runs
              on runs.id = fetches.crawl_run_id
            where runs.category = %s
              and runs.status = 'completed'
              and fetches.node_type = %s
              and fetches.outcome = 'fetched'
            order by fetches.fetched_at desc, fetches.id desc
            limit 1
            """,
            (category, node_type),
        ).fetchone()
    if row is None:
        return False, None
    return True, row[0] or None


def run(argv: list[str] | None = None, *, project_root: Path | None = None) -> int:
    root = (project_root or Path(__file__).resolve().parents[1]).resolve()
    args = parse_args(argv)
    _ensure_positive(args)
    settings = Settings.from_env(root)
    _assert_category_idle(settings.database_url, args.category)

    if args.resume_latest_completed:
        has_cursor, seed_path = _latest_completed_seed(
            settings.database_url,
            category=args.category,
            node_type=args.seed_node_type,
        )
        if not has_cursor:
            print(f"{args.category} has no completed stored cursor; skipping", flush=True)
            return 0
        if not seed_path:
            print(f"{args.category} cursor already exhausted; skipping", flush=True)
            return 0
    else:
        seed_path = args.seed_path
    assert seed_path is not None
    for batch_number in range(1, args.batches + 1):
        print(f"starting {args.category} cursor batch {batch_number}/{args.batches}", flush=True)
        run_id = _run_one_batch(root=root, args=args, seed_path=seed_path)
        seed_path = _next_page(settings.database_url, run_id=run_id, node_type=args.seed_node_type) or ""
        if not seed_path:
            print(f"{args.category} cursor exhausted after {batch_number} batch(es)", flush=True)
            break
        print(f"completed {args.category} batch {run_id}; next cursor stored", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
