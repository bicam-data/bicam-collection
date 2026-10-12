"""Review reconciliation queues, record decisions, and apply them.

Reconciliation surfaces cases the deterministic rules cannot settle: unmatched
or ambiguous Congress.gov/GovInfo pairs, GovInfo members without a Bioguide ID,
unmapped GovInfo committee codes.  A human decision for each case is a row in
a dbt seed under ``dbt_project/seeds/reconciliation/``; dbt reads those seeds
on every build.  This tool is the workflow around those seeds:

    reconcile.py queues                          open items per queue
    reconcile.py list QUEUE [--worksheet FILE]   show items, or export a worksheet
    reconcile.py decide QUEUE --key k=v ... --decision D [--value V] --reason R
    reconcile.py decide QUEUE --worksheet FILE   record every filled-in worksheet row
    reconcile.py apply [QUEUE ...]               load seeds and rebuild the queue models

``decide`` only edits seed CSVs (commit them like code).  ``apply`` reseeds and
rebuilds the path from each seed to its queue model under the shared ingestion
lock; the scheduled lanes rebuild the public tables on their next run.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import fcntl
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

import psycopg
from psycopg.rows import dict_row

from bicam_collection.config import Settings, load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SEED_DIR = PROJECT_ROOT / "dbt_project" / "seeds" / "reconciliation"
LOCK_PATH = Path(f"/run/user/{os.getuid()}/bicam-ingestion.lock")
WORKSHEET_DECISION_COLUMNS = ("decision", "value", "reason", "expires_at")
DEFAULT_EXPIRY_DAYS = 365


@dataclass(frozen=True)
class Decision:
    seed: str
    # Seed column that receives the decision name, if the seed records one.
    decision_column: str | None = None
    # Seed column that receives --value; decisions without one take no value.
    value_column: str | None = None
    fixed: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Queue:
    name: str
    description: str
    table: str
    keys: tuple[str, ...]
    # Which rows still need a decision.  ``decide`` accepts any row of the
    # table, so an earlier or automatic decision can be revisited.
    open_filter: str
    display: tuple[str, ...]
    decisions: dict[str, Decision]
    # The model whose rows close once the decision is loaded.
    dbt_model: str
    # Extra seed columns copied from the queue row, as SQL over that row.
    context: dict[str, str] = field(default_factory=dict)
    # Seed timestamp columns are dates in some seeds and timestamps in others.
    date_only: bool = False


DISPOSITIONS = ("confirmed_source_gap", "out_of_scope", "needs_source_refresh", "data_error")

QUEUES: dict[str, Queue] = {
    queue.name: queue
    for queue in (
        Queue(
            name="match-exceptions",
            description="Bills, committee reports, and hearings without an accepted cross-source match",
            table="public.fct_reconciliation_exceptions",
            keys=("entity", "canonical_id"),
            open_filter="exception_status = 'open'",
            display=("entity", "canonical_id", "missing_source", "match_decision", "review_disposition",
                     "latest_fetch_http_status", "congress_title", "govinfo_title"),
            decisions={
                disposition: Decision(
                    seed="reconciliation_exception_reviews",
                    decision_column="disposition",
                    fixed={"contract_version": "v1"},
                )
                for disposition in DISPOSITIONS
            },
            dbt_model="fct_reconciliation_exceptions",
        ),
        Queue(
            name="bill-matches",
            description="Ambiguous or rejected Congress.gov/GovInfo bill pairs",
            table="public.fct_bill_matches",
            keys=("congress_source_record_id", "govinfo_source_record_id"),
            open_filter="decision in ('ambiguous', 'rejected')",
            display=("canonical_bill_id", "decision", "decision_rule", "title_agrees",
                     "congress_title", "govinfo_title"),
            decisions={
                decision: Decision(
                    seed="bill_match_overrides",
                    decision_column="decision",
                    fixed={"contract_version": "v1"},
                )
                for decision in ("accepted", "rejected")
            },
            dbt_model="fct_bill_matches",
        ),
        Queue(
            name="committee-report-matches",
            description="Ambiguous or rejected Congress.gov/GovInfo committee-report pairs",
            table="public.fct_committee_report_matches",
            keys=("congress_source_record_id", "govinfo_source_record_id"),
            open_filter="decision in ('ambiguous', 'rejected')",
            display=("canonical_report_id", "decision", "decision_rule", "title_agrees",
                     "congress_title", "govinfo_title"),
            decisions={
                decision: Decision(
                    seed="committee_report_match_overrides",
                    decision_column="decision",
                    fixed={"contract_version": "v1"},
                )
                for decision in ("accepted", "rejected")
            },
            dbt_model="fct_committee_report_matches",
        ),
        Queue(
            name="member-identities",
            description="GovInfo committee-report members without a resolvable Bioguide ID",
            table="public.fct_govinfo_member_identity_exceptions",
            keys=("source_member_key",),
            open_filter="true",
            display=("source_member_key", "congress", "state", "member_name_raw", "role",
                     "committee_names", "bioguide_resolution_status"),
            decisions={
                "assign": Decision(seed="govinfo_member_identity_overrides", value_column="bioguide_id"),
            },
            dbt_model="fct_govinfo_member_identity_exceptions",
            context={
                "committee_id": "committee_ids[1]",
                "congress": "congress::text",
                "state": "state",
                "member_name": "member_name_raw",
            },
            date_only=True,
        ),
        Queue(
            name="member-references",
            description="GovInfo hearing/report participants with no Congress.gov member candidate",
            table="public.fct_govinfo_member_reference_identity_exceptions",
            keys=("source_member_reference_key",),
            open_filter="true",
            display=("source_member_reference_key", "congress", "state", "role", "member_name_raw",
                     "candidate_count", "resolution_status"),
            decisions={
                "reviewed_non_federal_participant": Decision(
                    seed="govinfo_member_reference_reviews",
                    decision_column="classification",
                ),
            },
            dbt_model="fct_govinfo_member_reference_identity_exceptions",
            date_only=True,
        ),
        Queue(
            name="committee-ids",
            description="GovInfo committee codes that do not map to a Congress.gov committee",
            table="public.fct_govinfo_committee_id_exceptions",
            keys=("source_committee_id",),
            open_filter="true",
            display=("source_committee_id", "normalization_status", "committee_names", "package_ids"),
            decisions={
                "map": Decision(seed="govinfo_committee_id_overrides", value_column="committee_id"),
                "reviewed_authority_exception": Decision(
                    seed="govinfo_committee_id_exceptions",
                    decision_column="classification",
                ),
            },
            dbt_model="fct_govinfo_committee_id_exceptions",
            date_only=True,
        ),
    )
}


@dataclass(frozen=True)
class DecisionRequest:
    key: dict[str, str]
    decision: str
    value: str | None
    reason: str
    expires_at: str | None  # YYYY-MM-DD, "never", or None for the default


def build_seed_row(
    queue: Queue,
    request: DecisionRequest,
    context: dict[str, str],
    *,
    reviewer: str,
    now: datetime,
) -> tuple[str, dict[str, str]]:
    """Return the seed name and the row that records ``request``."""

    if request.decision not in queue.decisions:
        allowed = ", ".join(sorted(queue.decisions))
        raise ValueError(f"{queue.name}: unknown decision {request.decision!r} (allowed: {allowed})")
    decision = queue.decisions[request.decision]
    if decision.value_column and not request.value:
        raise ValueError(f"{queue.name}: decision {request.decision!r} requires --value ({decision.value_column})")
    if not decision.value_column and request.value:
        raise ValueError(f"{queue.name}: decision {request.decision!r} takes no --value")
    if not request.reason.strip():
        raise ValueError(f"{queue.name}: a reason is required")

    if request.expires_at == "never":
        expires: date | None = None
    elif request.expires_at:
        expires = date.fromisoformat(request.expires_at)
    else:
        expires = now.date() + timedelta(days=DEFAULT_EXPIRY_DAYS)

    if queue.date_only:
        reviewed_at = now.date().isoformat()
        expires_at = expires.isoformat() if expires else ""
    else:
        reviewed_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        expires_at = f"{expires.isoformat()}T00:00:00Z" if expires else ""

    row = {**request.key, **context, **decision.fixed}
    if decision.decision_column:
        row[decision.decision_column] = request.decision
    if decision.value_column:
        row[decision.value_column] = request.value or ""
    row.update(reviewer=reviewer, reviewed_at=reviewed_at, reason=request.reason.strip(), expires_at=expires_at)
    return decision.seed, row


def upsert_seed_row(seed_path: Path, key_columns: tuple[str, ...], row: dict[str, str]) -> str:
    """Write ``row`` into the seed, replacing an earlier decision for the same key."""

    with seed_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        header = list(reader.fieldnames or [])
        existing = list(reader)

    action = "added"
    kept: list[dict[str, str]] = []
    for current in existing:
        if all(current[column] == row[column] for column in key_columns):
            action = "replaced"
            continue
        kept.append(current)
    kept.append(row)

    with seed_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        for current in kept:
            writer.writerow({column: current.get(column, "") for column in header})
    return action


def parse_key(queue: Queue, pairs: list[str]) -> dict[str, str]:
    key: dict[str, str] = {}
    for pair in pairs:
        name, separator, value = pair.partition("=")
        if not separator:
            raise ValueError(f"--key must look like name=value, got {pair!r}")
        key[name] = value
    if set(key) != set(queue.keys):
        raise ValueError(f"{queue.name}: --key must name exactly {', '.join(queue.keys)}")
    return key


def read_worksheet(queue: Queue, path: Path) -> list[DecisionRequest]:
    """Return a request for every worksheet row with a decision filled in."""

    requests = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if not (row.get("decision") or "").strip():
                continue
            requests.append(
                DecisionRequest(
                    key={column: row[column] for column in queue.keys},
                    decision=row["decision"].strip(),
                    value=(row.get("value") or "").strip() or None,
                    reason=row.get("reason") or "",
                    expires_at=(row.get("expires_at") or "").strip() or None,
                )
            )
    return requests


def fetch_queue_row(connection: psycopg.Connection, queue: Queue, key: dict[str, str]) -> dict[str, Any] | None:
    context_sql = "".join(f", ({expression})::text as {column}" for column, expression in queue.context.items())
    where = " and ".join(f"{column}::text = %({column})s" for column in queue.keys)
    sql = f"select 1 as found{context_sql} from {queue.table} where {where} limit 1"
    return connection.execute(sql, key).fetchone()


def default_reviewer() -> str:
    completed = subprocess.run(["git", "config", "user.name"], capture_output=True, text=True, cwd=PROJECT_ROOT)
    return completed.stdout.strip() or os.environ.get("USER", "unknown")


def command_queues(connection: psycopg.Connection) -> int:
    print(f"{'queue':<26} {'open':>9}  description")
    for queue in QUEUES.values():
        row = connection.execute(f"select count(*) as n from {queue.table} where {queue.open_filter}").fetchone()
        print(f"{queue.name:<26} {row['n']:>9}  {queue.description}")
    return 0


def command_list(connection: psycopg.Connection, queue: Queue, args: argparse.Namespace) -> int:
    columns = list(dict.fromkeys([*queue.keys, *queue.display]))
    where = "true" if args.include_reviewed else queue.open_filter
    sql = f"select {', '.join(columns)} from {queue.table} where {where} order by {', '.join(queue.keys)}"
    if args.limit:
        sql += f" limit {int(args.limit)}"
    rows = connection.execute(sql).fetchall()

    if args.worksheet:
        with args.worksheet.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=[*columns, *WORKSHEET_DECISION_COLUMNS], lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({column: "" if row[column] is None else row[column] for column in columns})
        allowed = ", ".join(sorted(queue.decisions))
        print(f"wrote {len(rows)} rows to {args.worksheet}")
        print(f"fill in decision ({allowed}), value, and reason; then run: reconcile.py decide {queue.name} --worksheet {args.worksheet}")
        return 0

    print(" | ".join(columns))
    for row in rows:
        print(" | ".join(_truncate(row[column]) for column in columns))
    print(f"-- {len(rows)} rows ({'all' if args.include_reviewed else 'open'})")
    return 0


def command_decide(connection: psycopg.Connection, queue: Queue, args: argparse.Namespace) -> int:
    if args.worksheet:
        requests = read_worksheet(queue, args.worksheet)
    else:
        if not args.decision or args.reason is None:
            raise ValueError("decide needs --worksheet, or --key, --decision and --reason")
        requests = [
            DecisionRequest(
                key=parse_key(queue, args.key or []),
                decision=args.decision,
                value=args.value,
                reason=args.reason,
                expires_at=args.expires,
            )
        ]

    reviewer = args.reviewer or default_reviewer()
    now = datetime.now(timezone.utc)
    # Validate everything before writing anything, so a bad worksheet row
    # never leaves the seeds half-updated.
    planned: list[tuple[str, dict[str, str]]] = []
    for request in requests:
        found = fetch_queue_row(connection, queue, request.key)
        if found is None:
            raise ValueError(f"{queue.name}: no row in {queue.table} for {request.key}")
        context = {column: found[column] or "" for column in queue.context}
        planned.append(build_seed_row(queue, request, context, reviewer=reviewer, now=now))

    for seed, row in planned:
        action = upsert_seed_row(SEED_DIR / f"{seed}.csv", queue.keys, row)
        print(f"{action} {seed}: {' '.join(row[column] for column in queue.keys)} -> {row}")
    print(f"recorded {len(planned)} decision(s); run `reconcile.py apply {queue.name}` to load them")
    return 0


def command_apply(queues: list[Queue], lock_timeout_seconds: int) -> int:
    seeds = sorted({decision.seed for queue in queues for decision in queue.decisions.values()})
    # Rebuild only models between a changed seed and its queue model; the
    # scheduled lanes reseed and rebuild the public tables on their own runs.
    paths = [f"{seed}+,+{queue.dbt_model}" for queue in queues for seed in
             sorted({decision.seed for decision in queue.decisions.values()})]
    dbt = ["uv", "run", "dbt", "--no-partial-parse"]
    project = ["--project-dir", "dbt_project", "--profiles-dir", "dbt_project"]

    # dbt reads DB_* from the environment, not from .env.
    load_dotenv(PROJECT_ROOT / ".env")
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("a") as lock:
        print(f"waiting up to {lock_timeout_seconds}s for the ingestion lock {LOCK_PATH}")
        _flock_with_timeout(lock, lock_timeout_seconds)
        for command in (
            [*dbt, "seed", *project, "--select", *seeds],
            [*dbt, "run", *project, "--select", *paths],
        ):
            print("+", " ".join(command))
            completed = subprocess.run(command, cwd=PROJECT_ROOT)
            if completed.returncode != 0:
                return completed.returncode
    return 0


def _flock_with_timeout(lock: Any, timeout_seconds: int) -> None:
    deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)
    while True:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if datetime.now(timezone.utc) >= deadline:
                raise SystemExit("ingestion lock is busy (a lane is running); try again later") from None
            time.sleep(5)


def _truncate(value: Any, width: int = 60) -> str:
    text = "" if value is None else str(value)
    return text if len(text) <= width else text[: width - 1] + "…"


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("queues", help="open items per queue")

    list_parser = commands.add_parser("list", help="show queue items or export a worksheet")
    list_parser.add_argument("queue", choices=sorted(QUEUES))
    list_parser.add_argument("--limit", type=int, default=50, help="0 for no limit (default 50)")
    list_parser.add_argument("--worksheet", type=Path, help="write a CSV worksheet instead of printing")
    list_parser.add_argument("--include-reviewed", action="store_true", help="include rows already decided")

    decide = commands.add_parser("decide", help="record decisions in the seed CSVs")
    decide.add_argument("queue", choices=sorted(QUEUES))
    decide.add_argument("--worksheet", type=Path, help="filled-in worksheet from `list --worksheet`")
    decide.add_argument("--key", action="append", metavar="NAME=VALUE", help="queue key column (repeat per column)")
    decide.add_argument("--decision")
    decide.add_argument("--value", help="e.g. the Bioguide ID for member-identities assign")
    decide.add_argument("--reason")
    decide.add_argument("--expires", help=f"YYYY-MM-DD or 'never' (default {DEFAULT_EXPIRY_DAYS} days)")
    decide.add_argument("--reviewer", help="default: git user.name")

    apply = commands.add_parser("apply", help="load seeds and rebuild queue models")
    apply.add_argument("queue", nargs="*", choices=sorted(QUEUES), help="default: all queues")
    apply.add_argument("--lock-timeout", type=int, default=900)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "apply":
        return command_apply([QUEUES[name] for name in args.queue or QUEUES], args.lock_timeout)

    database_url = Settings.from_env(PROJECT_ROOT).database_url
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as connection:
            if args.command == "queues":
                return command_queues(connection)
            queue = QUEUES[args.queue]
            if args.command == "list":
                return command_list(connection, queue, args)
            return command_decide(connection, queue, args)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
