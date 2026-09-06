"""Record one finished systemd lane invocation in the lane_runs table.

This runs from the ExecStopPost= hook that
deploy/systemd/bicam-.service.d/10-lane-status.conf adds to every bicam-*
service.  A failure here must not change the outcome of the lane it is
observing, so main() turns any exception into a single stderr line and
exits 0.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import uuid

import psycopg
from psycopg.types.json import Jsonb

from bicam_collection.config import Settings

# ExecStopPost= runs after the main process has exited but before the unit
# enters the inactive state, so ExecMainExitTimestamp already describes this
# run while InactiveEnterTimestamp still describes the previous one.  The
# latter is read only so it lands in details for diagnosis.
TIMESTAMP_PROPERTIES = (
    "ExecMainStartTimestamp",
    "ExecMainExitTimestamp",
    "InactiveEnterTimestamp",
)

# `systemctl show --timestamp=utc` prints e.g. "Sun 2026-09-06 21:07:57 UTC".
SYSTEMD_TIMESTAMP_FORMAT = "%a %Y-%m-%d %H:%M:%S UTC"


@dataclass(frozen=True, slots=True)
class LaneRun:
    unit: str
    invocation_id: uuid.UUID
    started_at: datetime | None
    finished_at: datetime
    service_result: str
    exit_code: str
    exit_status: str
    details: dict[str, str]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Insert one lane_runs row for a finished systemd unit."
    )
    parser.add_argument("--unit", required=True, help="Full unit name (%%n)")
    parser.add_argument(
        "--invocation-id", required=True, help="systemd $INVOCATION_ID"
    )
    parser.add_argument(
        "--service-result", required=True, help="systemd $SERVICE_RESULT"
    )
    parser.add_argument("--exit-code", required=True, help="systemd $EXIT_CODE")
    parser.add_argument(
        "--exit-status", required=True, help="systemd $EXIT_STATUS"
    )
    return parser.parse_args(argv)


def parse_systemd_timestamp(value: str) -> datetime | None:
    if value == "":
        return None
    return datetime.strptime(value, SYSTEMD_TIMESTAMP_FORMAT).replace(
        tzinfo=timezone.utc
    )


def parse_show_output(text: str) -> dict[str, str]:
    properties: dict[str, str] = {}
    for line in text.splitlines():
        key, _, value = line.partition("=")
        properties[key] = value
    return properties


def read_unit_properties(unit: str) -> dict[str, str]:
    command = ["systemctl", "--user", "show", "--timestamp=utc", unit]
    for name in TIMESTAMP_PROPERTIES:
        command.extend(["-p", name])
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return parse_show_output(completed.stdout)


def build_lane_run(
    *,
    unit: str,
    invocation_id: str,
    service_result: str,
    exit_code: str,
    exit_status: str,
    properties: dict[str, str],
    recorded_at: datetime,
) -> LaneRun:
    started_at = parse_systemd_timestamp(properties["ExecMainStartTimestamp"])
    # An exec failure before the main process starts leaves the exit
    # timestamp empty; the hook runs immediately afterwards, so "now" is
    # within a second of the truth.
    finished_at = parse_systemd_timestamp(properties["ExecMainExitTimestamp"])
    if finished_at is None:
        finished_at = recorded_at
    details = {
        "recorded_at": recorded_at.isoformat(),
        "exec_main_start_timestamp": properties["ExecMainStartTimestamp"],
        "exec_main_exit_timestamp": properties["ExecMainExitTimestamp"],
        "inactive_enter_timestamp": properties["InactiveEnterTimestamp"],
    }
    return LaneRun(
        unit=unit,
        invocation_id=uuid.UUID(invocation_id),
        started_at=started_at,
        finished_at=finished_at,
        service_result=service_result,
        exit_code=exit_code,
        exit_status=exit_status,
        details=details,
    )


def insert_lane_run(connection: psycopg.Connection, run: LaneRun) -> None:
    connection.execute(
        """
        INSERT INTO lane_runs (
            invocation_id, unit, started_at, finished_at,
            service_result, exit_code, exit_status, details
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run.invocation_id,
            run.unit,
            run.started_at,
            run.finished_at,
            run.service_result,
            run.exit_code,
            run.exit_status,
            Jsonb(run.details),
        ),
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        project_root = Path(__file__).resolve().parents[1]
        settings = Settings.from_env(project_root)
        run = build_lane_run(
            unit=args.unit,
            invocation_id=args.invocation_id,
            service_result=args.service_result,
            exit_code=args.exit_code,
            exit_status=args.exit_status,
            properties=read_unit_properties(args.unit),
            recorded_at=datetime.now(timezone.utc),
        )
        with psycopg.connect(settings.database_url, connect_timeout=10) as connection:
            connection.execute("set statement_timeout='10s'")
            insert_lane_run(connection, run)
    except Exception as error:  # noqa: BLE001 - the lane's own result must win
        print(
            f"record_lane_run: not recorded for {args.unit}: {error!r}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
