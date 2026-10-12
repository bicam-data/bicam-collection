"""Log a failed bicam-* service run (OnFailure= hook via alert-bicam-lane@.service).

Appends one JSON line to the alert log, which is the notification channel, and
stamps the matching lane_runs row.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess

import psycopg

from bicam_collection.config import Settings

JOURNAL_LINE_COUNT = 20


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append a lane failure alert line and mark the lane_runs row."
    )
    parser.add_argument("--unit", required=True, help="Full unit name (%%n)")
    parser.add_argument(
        "--log-path",
        type=Path,
        default=Path.home() / "bicam" / "reports" / "lane-alerts.log",
    )
    return parser.parse_args(argv)


def read_recent_journal(unit: str) -> list[str]:
    completed = subprocess.run(
        [
            "journalctl",
            "--user",
            "-u",
            unit,
            "-n",
            str(JOURNAL_LINE_COUNT),
            "--no-pager",
            "-o",
            "cat",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.splitlines()


def build_alert_line(unit: str, alerted_at: datetime, journal_lines: list[str]) -> str:
    alert = {
        "unit": unit,
        "alerted_at": alerted_at.isoformat(),
        "journal": journal_lines,
    }
    return json.dumps(alert) + "\n"


def mark_lane_run_alerted(
    connection: psycopg.Connection, unit: str, alerted_at: datetime
) -> int:
    # OnFailure= runs after ExecStopPost=, so the newest row is this failure.
    cursor = connection.execute(
        """
        UPDATE lane_runs
        SET details = details || jsonb_build_object('alerted_at', %s::text)
        WHERE invocation_id = (
            SELECT invocation_id
            FROM lane_runs
            WHERE unit = %s
            ORDER BY finished_at DESC
            LIMIT 1
        )
        """,
        (alerted_at.isoformat(), unit),
    )
    return cursor.rowcount


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    alerted_at = datetime.now(timezone.utc)
    line = build_alert_line(args.unit, alerted_at, read_recent_journal(args.unit))
    # Log first: a database outage may be why the lane failed.
    args.log_path.parent.mkdir(parents=True, exist_ok=True)
    with args.log_path.open("a", encoding="utf-8") as log:
        log.write(line)
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    with psycopg.connect(settings.database_url, connect_timeout=10) as connection:
        connection.execute("set statement_timeout='10s'")
        marked = mark_lane_run_alerted(connection, args.unit, alerted_at)
    print(f"alerted {args.unit}; lane_runs rows marked: {marked}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
