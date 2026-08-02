"""Inspect or explicitly abandon crawl runs that stopped without cleanup."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

from bicam_collection.config import Settings


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--categories", nargs="*", help="Limit recovery to these categories")
    parser.add_argument("--older-than-hours", type=float, default=1.0)
    parser.add_argument("--apply", action="store_true", help="Mark matching runs abandoned")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    stale_before = datetime.now(timezone.utc) - timedelta(hours=max(0.0, args.older_than_hours))
    categories = args.categories or []
    category_clause = "AND category = ANY(%s)" if categories else ""
    with psycopg.connect(settings.database_url) as conn:
        params: tuple[object, ...] = (stale_before, categories) if categories else (stale_before,)
        rows = conn.execute(
            f"""
            SELECT id, category, started_at
            FROM crawl_runs
            WHERE status = 'running'
              AND started_at < %s
              {category_clause}
            ORDER BY started_at
            """,
            params,
        ).fetchall()
        if args.apply and rows:
            ids = [row[0] for row in rows]
            conn.execute(
                """
                UPDATE crawl_runs
                SET status = 'abandoned', finished_at = NOW()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
            conn.commit()
    action = "abandoned" if args.apply else "would abandon"
    print(f"{action} {len(rows)} stale crawl run(s) older than {stale_before.isoformat()}")
    for run_id, category, started_at in rows:
        print(f"{run_id} {category} {started_at.isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
