"""Harvest the standalone Congress.gov ``/summaries`` feed in bounded windows.

The standalone feed is sorted by update date and defaults to a recent slice.
This runner explicitly supplies date windows, follows terminal pagination for
each window, and records completed windows in a small JSON checkpoint.  A
failed window is left incomplete so the next invocation retries it; overlap
between windows is safe because raw response URLs are idempotent and the dbt
model deduplicates summary identities.

This is bill-summary evidence ingestion.  It intentionally does not create a
new canonical entity or participate in bill reconciliation.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile

from bicam_collection.config import Settings
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions
from bicam_collection.runtime import base_url_for_spec, build_client
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import AsyncPostgresStore


@dataclass(frozen=True, slots=True)
class Window:
    start: datetime
    end: datetime

    @property
    def key(self) -> str:
        return f"{format_timestamp(self.start)}/{format_timestamp(self.end)}"


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def windows(start: datetime, end: datetime, *, window_days: int, overlap_hours: int) -> list[Window]:
    if end <= start:
        raise ValueError("window end must be later than start")
    if window_days < 1:
        raise ValueError("window-days must be positive")
    if overlap_hours < 0 or overlap_hours >= window_days * 24:
        raise ValueError("overlap-hours must be non-negative and smaller than one window")
    result: list[Window] = []
    cursor = start
    step = timedelta(days=window_days)
    overlap = timedelta(hours=overlap_hours)
    while cursor < end:
        window_end = min(cursor + step, end)
        result.append(Window(cursor, window_end))
        if window_end >= end:
            break
        cursor = window_end - overlap
    return result


def load_checkpoint(path: Path) -> set[str]:
    if not path.exists():
        return set()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("version") != 1 or payload.get("category") != "summaries":
        raise ValueError(f"unsupported summaries checkpoint: {path}")
    completed = payload.get("completed_windows", [])
    if not isinstance(completed, list) or any(not isinstance(value, str) for value in completed):
        raise ValueError(f"invalid completed_windows in checkpoint: {path}")
    return set(completed)


def save_checkpoint(path: Path, completed: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "category": "summaries",
        "completed_windows": sorted(completed),
        "updated_at": format_timestamp(datetime.now(timezone.utc)),
    }
    # Replace the checkpoint atomically so a host/process interruption cannot
    # leave a syntactically valid-looking but truncated frontier.
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = handle.name
    os.replace(temporary, path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="1789-01-01T00:00:00Z")
    parser.add_argument("--until", help="UTC upper bound; defaults to now minus --lag-hours")
    parser.add_argument("--lag-hours", type=float, default=2.0)
    parser.add_argument("--window-days", type=int, default=31)
    parser.add_argument("--overlap-hours", type=int, default=24)
    parser.add_argument("--max-windows", type=int, default=12)
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--request-interval", type=float, default=0.25)
    parser.add_argument("--key-wait-timeout", type=float, default=1200.0)
    parser.add_argument("--active-key-budget", type=int, default=None)
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=Path("reports/congress-summaries-checkpoint.json"),
    )
    parser.add_argument("--reset-checkpoint", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    if args.lag_hours < 0:
        raise ValueError("lag-hours must be non-negative")
    if args.max_windows < 1 or args.window_days < 1 or args.page_size < 1 or args.concurrency < 1:
        raise ValueError("max-windows, window-days, page-size, and concurrency must be positive")
    if args.overlap_hours < 0 or args.overlap_hours >= args.window_days * 24:
        raise ValueError("overlap-hours must be non-negative and smaller than one window")
    if args.request_interval < 0 or args.key_wait_timeout <= 0:
        raise ValueError("request-interval must be non-negative and key-wait-timeout must be positive")
    if args.active_key_budget is not None and args.active_key_budget < 1:
        raise ValueError("active-key-budget must be positive")


async def run(args: argparse.Namespace, *, project_root: Path) -> int:
    validate_args(args)
    start = parse_timestamp(args.start)
    until = (
        parse_timestamp(args.until)
        if args.until
        else datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=args.lag_hours)
    )
    planned = windows(start, until, window_days=args.window_days, overlap_hours=args.overlap_hours)
    completed = set() if args.reset_checkpoint else load_checkpoint(args.checkpoint)
    pending = [window for window in planned if window.key not in completed][: args.max_windows]
    print(f"[summaries] planned={len(planned)} completed={len(completed)} pending={len(pending)}", flush=True)
    if args.dry_run:
        for window in pending:
            print(f"[summaries] window={window.key}", flush=True)
        return 0
    if not pending:
        return 0

    settings = Settings.from_env(project_root)
    spec = load_spec(project_root / "specs" / "summaries.yaml")
    client = build_client(
        spec=spec,
        env_path=settings.env_path,
        use_async=True,
        key_wait_timeout=args.key_wait_timeout,
        request_interval=args.request_interval,
        active_key_budget=args.active_key_budget,
    )
    store = AsyncPostgresStore(settings.database_url)
    try:
        for window in pending:
            crawler = AsyncSpecCrawler(
                base_url=base_url_for_spec(settings, spec),
                spec=spec,
                client=client,
                store=store,
                concurrency=args.concurrency,
            )
            run_id = await crawler.run(
                CrawlOptions(
                    mode="incremental",
                    from_datetime=format_timestamp(window.start),
                    to_datetime=format_timestamp(window.end),
                    max_pages=None,
                    page_size=args.page_size,
                )
            )
            completed.add(window.key)
            save_checkpoint(args.checkpoint, completed)
            print(f"[summaries] completed window={window.key} run={run_id}", flush=True)
    finally:
        await store.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(run(args, project_root=Path(__file__).resolve().parents[1]))


if __name__ == "__main__":
    raise SystemExit(main())
