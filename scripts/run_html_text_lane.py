"""Resumable HTML text extraction lane.

Each batch is small enough to stop safely; one lexical cursor per source table
is persisted in reports/html-text/checkpoint.json and resumes after a restart
or usage-window reset.  Fetching reads public HTML renditions and consumes no
Congress.gov API keys, so it runs without the shared ingestion lock; only dbt
propagation touches the shared intermediate tables and takes the lock.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any

import psycopg

from bicam_collection import config

REPO_ROOT = Path(__file__).resolve().parents[1]
LOCK_PATH = Path(f"/run/user/{os.getuid()}/bicam-ingestion.lock")

# Rotate one public text table per batch.  A single lexical cursor could spend
# days in the hearing URL range before reaching bills, amendments, or GovInfo
# bill versions.  Successful rows are filtered by the database, so an old
# checkpoint is safely restarted at the beginning of the fair queue.
SOURCE_TABLES = (
    "amendments_texts",
    "bills_texts",
    "committee_prints_texts",
    "committee_reports_texts",
    "hearings_texts",
    "bills_versions",
)

# Rebuild each text model's selected child-state ancestors as well.  The
# public text tables depend on resolver-backed intermediate tables (most
# visibly committee-print text); selecting only the leaf can otherwise leave a
# freshly harvested raw response invisible to the public product.  Keep this
# graph explicit instead of selecting every ancestor of each leaf: the
# ``+bills_texts`` form also rebuilds the entire bills/reconciliation graph,
# which makes a bounded HTML checkpoint spend its timeout on unrelated title
# and match models.  These are the raw/current, shared child-state,
# text-specific, and public relations required for the six HTML sources;
# root/public identity tables are refreshed by their source lanes.
DBT_SELECT = (
    "int_raw_response_versions", "int_raw_responses__current",
    "int_congress_child_response_selection", "int_congress_child_response_selections",
    "int_congress_amendment_text_versions", "int_congress_amendment_text_formats",
    "stg_congress__amendment_texts", "amendments_texts",
    "int_congress_bill_text_versions", "int_congress_bill_text_formats",
    "stg_congress__bill_texts", "bills_texts",
    "int_congress_committee_print_text_formats", "stg_congress__committee_print_texts",
    "int_committee_prints__congress", "int_committee_print_public_id_map", "committee_prints_texts",
    "int_congress_committee_report_records", "int_congress_committee_report_text_formats",
    "stg_congress__committee_report_detail", "stg_congress__bill_committeereports",
    "stg_congress__committee_report_texts", "int_committee_reports__congress",
    "int_committee_reports__govinfo", "committee_reports_texts",
    "int_hearings__congress", "int_congress_hearing_public_id_map",
    "int_govinfo_hearing_identity_map", "int_govinfo_hearing_granules",
    "stg_congress__hearing_texts", "hearings_texts",
    "bills_versions",
)
DBT_TEST_SELECT = (
    "html_successful_extractions_have_text",
    "html_successful_current_prefers_last_success",
)
# Mirrors ``timeout --signal=TERM --kill-after=20s``: a dbt client that ignores
# TERM is killed rather than allowed to outlive the bounded checkpoint.
DBT_KILL_AFTER_SECONDS = 20.0


def utc_now(fmt: str) -> str:
    return datetime.now(timezone.utc).strftime(fmt)


def load_dotenv(path: Path) -> None:
    # ``set -a; . ./.env`` semantics: every assignment becomes exported and
    # overrides an existing variable.  The file is plain KEY=VALUE lines with
    # blank lines and ``#`` comments.
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        os.environ[key] = value


def write_json(path: Path, payload: dict[str, Any]) -> None:
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, path)


def append_run_log(output_dir: Path, line: str) -> None:
    with (output_dir / "run.log").open("a", encoding="utf-8") as log:
        log.write(f"{utc_now('%Y-%m-%dT%H:%M:%SZ')} {line}\n")


@dataclass
class Settings:
    output_dir: Path
    batch_limit: int
    max_batches: int
    fetch_options: list[str]
    dbt_batch_interval: int
    skip_dbt: bool
    dbt_force: bool
    dbt_timeout_seconds: float
    dbt_statement_timeout_ms: str
    dbt_test_timeout_seconds: float
    dbt_test_statement_timeout_ms: str
    lock_timeout_seconds: float
    lock_already_held: bool

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        return cls(
            output_dir=Path(env.get("BICAM_HTML_REPORT_DIR") or REPO_ROOT / ".." / "reports" / "html-text"),
            batch_limit=int(env.get("BICAM_HTML_BATCH_LIMIT", "500")),
            max_batches=int(env.get("BICAM_HTML_MAX_BATCHES", "1")),
            fetch_options=[
                "--limit", env.get("BICAM_HTML_BATCH_LIMIT", "500"),
                "--concurrency", env.get("BICAM_HTML_CONCURRENCY", "12"),
                "--request-interval", env.get("BICAM_HTML_REQUEST_INTERVAL", "0.08"),
                "--congress-request-interval", env.get("BICAM_HTML_CONGRESS_REQUEST_INTERVAL", "1.0"),
                "--govinfo-request-interval", env.get("BICAM_HTML_GOVINFO_REQUEST_INTERVAL", "0.25"),
                "--refresh-days", env.get("BICAM_HTML_REFRESH_DAYS", "30"),
                "--retry-failed-days", env.get("BICAM_HTML_RETRY_FAILED_DAYS", "1"),
                "--timeout", env.get("BICAM_HTML_REQUEST_TIMEOUT", "12"),
                "--max-bytes", env.get("BICAM_HTML_MAX_BYTES", "20000000"),
            ],
            dbt_batch_interval=int(env.get("BICAM_HTML_DBT_BATCH_INTERVAL", "6")),
            skip_dbt=env.get("BICAM_HTML_SKIP_DBT", "0") == "1",
            dbt_force=env.get("BICAM_HTML_DBT_FORCE", "0") == "1",
            # The resolver-backed bill-text-version model needs more than ten
            # minutes on the production archive.  Keep propagation bounded, but
            # allow one complete graph attempt to finish rather than repeatedly
            # cancelling during its largest table.  PostgreSQL stops the
            # statement slightly before the client timeout so cancellation
            # cannot leave a server-side table build running after exit.
            dbt_timeout_seconds=float(env.get("BICAM_HTML_DBT_TIMEOUT_SECONDS", "1800")),
            dbt_statement_timeout_ms=env.get("BICAM_HTML_DBT_STATEMENT_TIMEOUT_MS", "1740000"),
            dbt_test_timeout_seconds=float(env.get("BICAM_HTML_DBT_TEST_TIMEOUT_SECONDS", "300")),
            dbt_test_statement_timeout_ms=env.get("BICAM_HTML_DBT_TEST_STATEMENT_TIMEOUT_MS", "270000"),
            lock_timeout_seconds=float(env.get("BICAM_HTML_LOCK_TIMEOUT_SECONDS", "900")),
            lock_already_held=env.get("BICAM_INGESTION_LOCK_HELD", "0") == "1",
        )


@dataclass
class Checkpoint:
    path: Path
    # The most recent fetch result (``requested``/``statuses``) plus the lane
    # keys below; written after every batch so a stop at any point resumes.
    payload: dict[str, Any]
    last_urls: dict[str, str]
    next_source_table: str
    pending_dbt: int
    dbt_batches_since_last: int

    @classmethod
    def load(cls, path: Path) -> Checkpoint:
        checkpoint = cls(
            path=path,
            payload={},
            last_urls={table: "" for table in SOURCE_TABLES},
            next_source_table=SOURCE_TABLES[0],
            pending_dbt=0,
            dbt_batches_since_last=0,
        )
        if not path.is_file() or path.stat().st_size == 0:
            return checkpoint
        payload = json.loads(path.read_text(encoding="utf-8"))
        checkpoint.payload = payload
        tables = payload.get("last_urls")
        if isinstance(tables, dict):
            for table, value in tables.items():
                if table in checkpoint.last_urls:
                    checkpoint.last_urls[table] = value or ""
        if payload.get("next_source_table") in SOURCE_TABLES:
            checkpoint.next_source_table = payload["next_source_table"]
        if payload.get("pending_dbt"):
            checkpoint.pending_dbt = 1
        counter = payload.get("dbt_batches_since_last")
        if isinstance(counter, int) and counter >= 0:
            checkpoint.dbt_batches_since_last = counter
        return checkpoint

    def save(self) -> None:
        self.payload["next_source_table"] = self.next_source_table
        self.payload["pending_dbt"] = self.pending_dbt
        self.payload["dbt_batches_since_last"] = self.dbt_batches_since_last
        self.payload["last_urls"] = dict(self.last_urls)
        self.payload.pop("last_url", None)
        write_json(self.path, self.payload)


def fetch_batch(settings: Settings, source_table: str, after: str) -> dict[str, Any]:
    command = [sys.executable, "scripts/fetch_html_text.py", "--source-table", source_table, *settings.fetch_options]
    if after:
        command += ["--after", after]
    completed = subprocess.run(command, stdout=subprocess.PIPE, text=True)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)
    result_line = completed.stdout.rstrip("\n")
    append_run_log(settings.output_dir, result_line)
    result: dict[str, Any] = json.loads(result_line)
    return result


def run_batches(settings: Settings, checkpoint: Checkpoint) -> None:
    # Count only full/useful batches against the configured cap.  A short
    # batch means that table is exhausted (or has no currently eligible URLs),
    # so it is reset and skipped without making the next useful table wait for
    # another timer tick.  The finite attempt bound prevents an all-exhausted
    # queue from turning the fairness loop into an infinite cycle.
    useful_batches = 0
    max_attempts = settings.max_batches + len(SOURCE_TABLES)
    for _attempt in range(max_attempts):
        if useful_batches >= settings.max_batches:
            break
        current_source_table = checkpoint.next_source_table
        result = fetch_batch(settings, current_source_table, checkpoint.last_urls[current_source_table])
        if result.get("statuses", {}).get("success", 0) > 0:
            checkpoint.pending_dbt = 1
            checkpoint.dbt_batches_since_last += 1
        checkpoint.last_urls[current_source_table] = result.get("last_url") or ""
        table_index = SOURCE_TABLES.index(current_source_table)
        checkpoint.next_source_table = SOURCE_TABLES[(table_index + 1) % len(SOURCE_TABLES)]
        checkpoint.payload = result
        checkpoint.save()
        if result.get("requested", 0) < settings.batch_limit:
            # A complete pass for this table starts from the beginning next
            # time so newly added URLs and URLs past the refresh age are
            # reconsidered.
            checkpoint.last_urls[current_source_table] = ""
            checkpoint.save()
            continue
        useful_batches += 1


def acquire_ingestion_lock(timeout_seconds: float) -> IO[str] | None:
    handle = LOCK_PATH.open("a")
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return handle
        except BlockingIOError:
            if time.monotonic() >= deadline:
                handle.close()
                return None
            time.sleep(1)


def run_bounded(command: list[str], env: dict[str, str], timeout_seconds: float) -> int:
    # ``timeout --signal=TERM --kill-after=20s`` semantics, including its exit
    # statuses: 124 when TERM ended the client, 137 when KILL was needed.
    process = subprocess.Popen(command, env=env)
    try:
        status = process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.terminate()
    else:
        # A client killed by a signal reports 128+signal in a shell.
        return status if status >= 0 else 128 - status
    try:
        process.wait(timeout=DBT_KILL_AFTER_SECONDS)
        return 124
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        return 137


def cancel_dbt_backends(app_name: str) -> None:
    # A client-side timeout can leave a PostgreSQL backend alive until TCP
    # keepalive notices the disconnect.  Cancel only this checkpoint's unique
    # application name; never touch another ingestion lane.
    database_url = config.Settings.from_env(REPO_ROOT).database_url

    def run_statement(statement: str) -> None:
        with contextlib.suppress(psycopg.Error), psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(statement, {"app_name": app_name})

    # The first cancellation can race the client-side timeout while its
    # connection is still unwinding.  A short second pass catches that backend
    # without broad termination or touching another run.
    for cancel_attempt in (1, 2):
        run_statement(
            "select pg_cancel_backend(pid) from pg_stat_activity"
            " where application_name = %(app_name)s and pid <> pg_backend_pid()"
        )
        if cancel_attempt == 1:
            time.sleep(2)
    # A backend in uninterruptible I/O can survive cancellation after the
    # client has exited.  Terminate only this exact abandoned application name.
    run_statement(
        "select pg_terminate_backend(pid) from pg_stat_activity"
        " where application_name = %(app_name)s and pid <> pg_backend_pid()"
    )


def run_dbt(subcommand: str, select: tuple[str, ...], app_name: str, statement_timeout_ms: str, timeout_seconds: float) -> int:
    env = dict(os.environ)
    env["PGAPPNAME"] = app_name
    env["PGOPTIONS"] = f"{os.environ.get('PGOPTIONS', '')} -c statement_timeout={statement_timeout_ms}"
    command = [
        "uv", "run", "dbt", subcommand, "--no-partial-parse",
        "--project-dir", "dbt_project", "--profiles-dir", "dbt_project",
        "--select", *select,
    ]
    status = run_bounded(command, env, timeout_seconds)
    if status != 0:
        cancel_dbt_backends(app_name)
    return status


def propagate(settings: Settings, checkpoint: Checkpoint) -> None:
    lock = None
    if not settings.lock_already_held:
        lock = acquire_ingestion_lock(settings.lock_timeout_seconds)
        if lock is None:
            append_run_log(settings.output_dir, '{"event":"lock_busy","retry":"next_timer"}')
            print("html_text_lock_busy; retrying on the next timer interval", flush=True)
            return
    app_name = f"bicam-html-dbt-{utc_now('%Y%m%dT%H%M%SZ')}-{os.getpid()}"
    # A systemd start-timeout can terminate the driver after its child client
    # exits.  Turn TERM/INT into an ordinary exit so the finally clause below
    # cannot leave an unowned server-side dbt backend behind.
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(143))
    signal.signal(signal.SIGINT, lambda signum, frame: sys.exit(130))
    try:
        run_status = run_dbt("run", DBT_SELECT, app_name, settings.dbt_statement_timeout_ms, settings.dbt_timeout_seconds)
        test_status = 0
        if run_status == 0:
            test_status = run_dbt(
                "test", DBT_TEST_SELECT, f"{app_name}-test",
                settings.dbt_test_statement_timeout_ms, settings.dbt_test_timeout_seconds,
            )
        else:
            print(f"html_dbt_test_skipped=1 reason=dbt_run_status_{run_status}", flush=True)
        print(f"html_dbt_run_status={run_status}", flush=True)
        print(f"html_dbt_test_status={test_status}", flush=True)
        if run_status == 0 and test_status == 0:
            checkpoint.pending_dbt = 0
        else:
            # Keep the extraction cursor and the pending flag durable.  The
            # next checkpoint retries propagation without discarding
            # successfully fetched HTML or pretending the public models are
            # current.  Zeroing the counter avoids retrying the same expensive
            # graph on every short extraction tick; new successful batches
            # advance it again and BICAM_HTML_DBT_FORCE=1 can still request an
            # immediate refresh.
            print("html_dbt_pending=1 reason=propagation_failed", flush=True)
        checkpoint.dbt_batches_since_last = 0
        checkpoint.save()
    finally:
        cancel_dbt_backends(app_name)
        if lock is not None:
            lock.close()


def main() -> int:
    os.chdir(REPO_ROOT)
    load_dotenv(REPO_ROOT / ".env")
    settings = Settings.from_env()
    interval_text = os.environ.get("BICAM_HTML_DBT_BATCH_INTERVAL", "6")
    if not re.fullmatch(r"[1-9][0-9]*", interval_text):
        print(f"invalid BICAM_HTML_DBT_BATCH_INTERVAL={interval_text} (expected positive integer)", file=sys.stderr)
        return 2
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = Checkpoint.load(settings.output_dir / "checkpoint.json")

    run_batches(settings, checkpoint)

    # A batch containing only HTTP/fetch failures cannot change the public
    # tables, so the expensive rebuild waits for successful rows, and then for
    # the batch interval or the end of a fair-queue cycle.
    cycle_boundary = checkpoint.next_source_table == SOURCE_TABLES[0]
    propagation_due = (
        checkpoint.dbt_batches_since_last >= settings.dbt_batch_interval
        or settings.dbt_force
        or (cycle_boundary and checkpoint.dbt_batches_since_last > 0)
    )
    if not settings.skip_dbt and checkpoint.pending_dbt == 1 and propagation_due:
        propagate(settings, checkpoint)
    elif checkpoint.pending_dbt == 1:
        print(
            f"html_dbt_deferred=1 batches_since_last={checkpoint.dbt_batches_since_last}"
            f" interval={settings.dbt_batch_interval}",
            flush=True,
        )

    # Keep a per-public-table checkpoint alongside the URL cursor.  This is
    # intentionally non-fatal: extraction results and the cursor are already
    # durable, and a report query must not stop the next batch.
    subprocess.run([sys.executable, "scripts/report_html_text_progress.py", "--output-dir", str(settings.output_dir)])

    print(f"html_text_checkpoint={checkpoint.path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
