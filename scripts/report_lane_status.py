"""Write a per-unit status report for the systemd BICAM lanes.

Combines the lane_runs history recorded by scripts/record_lane_run.py with
the live timer schedule from systemctl.  Read-only against the database.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile

import psycopg

from bicam_collection.config import Settings

# Enough history per unit to count a long failure streak without loading the
# whole table; anything longer is reported as 100+.
HISTORY_PER_UNIT = 100

LANE_RUNS_SQL = """
SELECT unit, started_at, finished_at, service_result, exit_code, exit_status
FROM (
    SELECT
        unit, started_at, finished_at, service_result, exit_code, exit_status,
        row_number() OVER (PARTITION BY unit ORDER BY finished_at DESC) AS recency
    FROM lane_runs
) ranked
WHERE recency <= %s
ORDER BY unit, finished_at DESC
"""


@dataclass(frozen=True, slots=True)
class LaneRunRow:
    unit: str
    started_at: datetime | None
    finished_at: datetime
    service_result: str
    exit_code: str
    exit_status: str


@dataclass(frozen=True, slots=True)
class UnitStatus:
    unit: str
    last_result: str | None
    last_exit: str | None
    last_finished_at: str | None
    last_duration_seconds: float | None
    consecutive_failures: int
    next_fire_at: str | None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write lane-status.md and lane-status.json for the BICAM lanes."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.home() / "bicam" / "reports",
    )
    return parser.parse_args(argv)


def parse_timer_schedule(timers_json: str) -> dict[str, datetime | None]:
    """Map each bicam-* service to the next realtime fire of its timer."""
    schedule: dict[str, datetime | None] = {}
    for timer in json.loads(timers_json):
        service = timer["activates"]
        if not service.startswith("bicam-"):
            continue
        next_microseconds = timer["next"]
        if next_microseconds is None:
            schedule[service] = None
        else:
            schedule[service] = datetime.fromtimestamp(
                next_microseconds / 1_000_000, tz=timezone.utc
            )
    return schedule


def read_timer_schedule() -> dict[str, datetime | None]:
    completed = subprocess.run(
        ["systemctl", "--user", "list-timers", "--all", "--output=json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return parse_timer_schedule(completed.stdout)


def summarize_units(
    rows: list[LaneRunRow], schedule: dict[str, datetime | None]
) -> list[UnitStatus]:
    """Fold newest-first rows into one status per unit.

    Units that only appear in the timer schedule are included with no run
    history, so a lane that has never recorded a run is still visible.
    """
    rows_by_unit: dict[str, list[LaneRunRow]] = {}
    for row in rows:
        rows_by_unit.setdefault(row.unit, []).append(row)

    statuses: list[UnitStatus] = []
    for unit in sorted(set(rows_by_unit) | set(schedule)):
        next_fire = schedule.get(unit)
        next_fire_at = next_fire.isoformat() if next_fire is not None else None
        history = rows_by_unit.get(unit, [])
        if not history:
            statuses.append(
                UnitStatus(
                    unit=unit,
                    last_result=None,
                    last_exit=None,
                    last_finished_at=None,
                    last_duration_seconds=None,
                    consecutive_failures=0,
                    next_fire_at=next_fire_at,
                )
            )
            continue

        newest = history[0]
        duration_seconds: float | None = None
        if newest.started_at is not None:
            duration_seconds = (newest.finished_at - newest.started_at).total_seconds()

        consecutive_failures = 0
        for row in history:
            if row.service_result == "success":
                break
            consecutive_failures += 1

        statuses.append(
            UnitStatus(
                unit=unit,
                last_result=newest.service_result,
                last_exit=f"{newest.exit_code}/{newest.exit_status}",
                last_finished_at=newest.finished_at.isoformat(),
                last_duration_seconds=duration_seconds,
                consecutive_failures=consecutive_failures,
                next_fire_at=next_fire_at,
            )
        )
    return statuses


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    whole = int(seconds)
    hours, remainder = divmod(whole, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def render_markdown(statuses: list[UnitStatus], generated_at: datetime) -> str:
    lines = [
        "# BICAM lane status",
        "",
        f"Generated: {generated_at.isoformat()}",
        "",
        "| Unit | Last result | Exit | Last finished | Duration | Consecutive failures | Next fire |",
        "|---|---|---|---|---|---|---|",
    ]
    for status in statuses:
        failures = str(status.consecutive_failures)
        if status.consecutive_failures >= HISTORY_PER_UNIT:
            failures = f"{HISTORY_PER_UNIT}+"
        lines.append(
            "| "
            + " | ".join(
                [
                    status.unit,
                    status.last_result or "never ran",
                    status.last_exit or "-",
                    status.last_finished_at or "-",
                    format_duration(status.last_duration_seconds),
                    failures,
                    status.next_fire_at or "-",
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def render_json(statuses: list[UnitStatus], generated_at: datetime) -> str:
    payload = {
        "generated_at": generated_at.isoformat(),
        "units": [asdict(status) for status in statuses],
    }
    return json.dumps(payload, indent=2) + "\n"


def write_atomically(path: Path, content: str) -> None:
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as temporary:
        temporary.write(content)
        temporary_path = temporary.name
    os.replace(temporary_path, path)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    generated_at = datetime.now(timezone.utc)
    with psycopg.connect(settings.database_url, connect_timeout=10) as connection:
        connection.execute("set statement_timeout='30s'")
        rows = [
            LaneRunRow(*record)
            for record in connection.execute(LANE_RUNS_SQL, (HISTORY_PER_UNIT,)).fetchall()
        ]
    statuses = summarize_units(rows, read_timer_schedule())
    args.output_dir.mkdir(parents=True, exist_ok=True)
    markdown = render_markdown(statuses, generated_at)
    write_atomically(args.output_dir / "lane-status.md", markdown)
    write_atomically(args.output_dir / "lane-status.json", render_json(statuses, generated_at))
    print(markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
