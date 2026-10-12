"""Resumable HTML text extraction lane, run by the bicam-html-text timer.

One cursor per source table is kept in reports/html-text/checkpoint.json.
Fetching uses public HTML renditions (no API keys) and runs without the shared
ingestion lock; only the dbt propagation step takes it.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
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

# Rotated one batch at a time so no table starves the rest.
SOURCE_TABLES = (
    "amendments_texts",
    "bills_texts",
    "committee_prints_texts",
    "committee_reports_texts",
    "hearings_texts",
    "bills_versions",
)

# The text tables plus the intermediates between them and the raw responses.
# Listed explicitly: +bills_texts would rebuild all of bills/reconciliation.
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
DBT_KILL_AFTER_SECONDS = 20.0


def utc_now(fmt: str) -> str:
    return datetime.now(timezone.utc).strftime(fmt)


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
        reports_dir = Path(env.get("BICAM_REPORTS_DIR") or REPO_ROOT.parent / "reports")
        dbt_batch_interval = int(env.get("BICAM_HTML_DBT_BATCH_INTERVAL", "6"))
        if dbt_batch_interval < 1:
            raise ValueError(f"BICAM_HTML_DBT_BATCH_INTERVAL must be positive, got {dbt_batch_interval}")
        return cls(
            output_dir=Path(env.get("BICAM_HTML_REPORT_DIR") or reports_dir / "html-text"),
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
            dbt_batch_interval=dbt_batch_interval,
            skip_dbt=env.get("BICAM_HTML_SKIP_DBT", "0") == "1",
            dbt_force=env.get("BICAM_HTML_DBT_FORCE", "0") == "1",
            # Statement timeouts sit just inside the client timeouts so a
            # killed dbt leaves no query running.
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
    # The latest fetch result plus the cursor fields below, saved after every batch.
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
    # Only full batches count against the cap; a short batch means the table
    # is exhausted, so its cursor resets and the next table runs. The attempt
    # bound ends an all-exhausted loop.
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
    """Like ``timeout --kill-after``: 124 if TERM ended the command, 137 if KILL was needed."""
    process = subprocess.Popen(command, env=env)
    try:
        status = process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.terminate()
    else:
        return status if status >= 0 else 128 - status
    try:
        process.wait(timeout=DBT_KILL_AFTER_SECONDS)
        return 124
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        return 137


def cancel_dbt_backends(app_name: str) -> None:
    """Stop backends a killed dbt left behind, matched by this run's unique application name.

    Cancels twice (the first can race the client disconnect), then terminates
    any stuck in I/O.
    """
    database_url = config.Settings.from_env(REPO_ROOT).database_url

    def run_statement(statement: str) -> None:
        with contextlib.suppress(psycopg.Error), psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(statement, {"app_name": app_name})

    for cancel_attempt in (1, 2):
        run_statement(
            "select pg_cancel_backend(pid) from pg_stat_activity"
            " where application_name = %(app_name)s and pid <> pg_backend_pid()"
        )
        if cancel_attempt == 1:
            time.sleep(2)
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
    # Turn a systemd TERM into a normal exit so the cleanup below still runs.
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
            # Stay pending. Resetting the counter below waits for new batches
            # (or BICAM_HTML_DBT_FORCE=1) before retrying the same graph.
            print("html_dbt_pending=1 reason=propagation_failed", flush=True)
        checkpoint.dbt_batches_since_last = 0
        checkpoint.save()
    finally:
        cancel_dbt_backends(app_name)
        if lock is not None:
            lock.close()


def main() -> int:
    os.chdir(REPO_ROOT)
    os.environ.update(config.parse_dotenv(REPO_ROOT / ".env"))
    settings = Settings.from_env()
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = Checkpoint.load(settings.output_dir / "checkpoint.json")

    run_batches(settings, checkpoint)

    # Rebuild only after new text arrived, and then every N batches or at the
    # end of a full rotation.
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

    # Non-fatal: results and cursors are already saved.
    subprocess.run([sys.executable, "scripts/report_html_text_progress.py", "--output-dir", str(settings.output_dir)])

    print(f"html_text_checkpoint={checkpoint.path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
