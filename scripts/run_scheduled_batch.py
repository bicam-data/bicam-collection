from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import sys
from typing import Iterator

import psycopg

from bicam_collection.config import Settings
from bicam_collection.contracts import load_contract_document, validate_schedule
from bicam_collection.spec_loader import discover_spec_categories


def load_schedule(project_root: Path) -> dict:
    schedule = load_contract_document(project_root / "contracts" / "schedules.yaml")
    validate_schedule(schedule, set(discover_spec_categories(project_root / "specs")))
    return schedule


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one bounded, lock-protected collection batch.")
    parser.add_argument("--job", default="bounded_congress")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without acquiring the database lock")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def build_commands(*, project_root: Path, job: dict, verbose: bool) -> list[list[str]]:
    crawl_command = [
        sys.executable,
        str(project_root / "scripts" / "run_multi_crawl.py"),
        "--categories",
        *job["categories"],
        "--mode",
        job["mode"],
        "--max-pages",
        str(job["max_pages"]),
        "--page-size",
        str(job["page_size"]),
        "--concurrency",
        str(job["concurrency"]),
        "--category-workers",
        str(job["category_workers"]),
    ]
    if job.get("overlap_hours") is not None:
        crawl_command.extend(["--overlap-hours", str(job["overlap_hours"])])
    dbt_command = [
        "dbt",
        "build",
        "--project-dir",
        str(project_root / "dbt_project"),
        "--select",
        job["dbt_select"],
        "--no-partial-parse",
    ]
    quality_command = [
        sys.executable,
        str(project_root / "scripts" / "check_quality_gates.py"),
        "--max-open-exceptions",
        str(job["max_open_exceptions"]),
        "--max-unsatisfied-fetches",
        str(job["max_unsatisfied_fetches"]),
        "--max-missing-full-scope-partitions",
        str(job.get("max_missing_full_scope_partitions", 0)),
        "--max-missing-root-items",
        str(job.get("max_missing_root_items", 0)),
        "--max-missing-child-items",
        str(job.get("max_missing_child_items", 0)),
    ]
    if verbose:
        print(json.dumps({"crawl": crawl_command, "dbt": dbt_command, "quality_gate": quality_command}, indent=2))
    return [crawl_command, dbt_command, quality_command]


@contextmanager
def advisory_lock(database_url: str, lock_key: str) -> Iterator[None]:
    with psycopg.connect(database_url) as connection:
        acquired = connection.execute("select pg_try_advisory_lock(hashtext(%s))", (lock_key,)).fetchone()[0]
        if not acquired:
            raise RuntimeError(f"another scheduled batch already holds lock '{lock_key}'")
        try:
            yield
        finally:
            connection.execute("select pg_advisory_unlock(hashtext(%s))", (lock_key,))


def run(argv: list[str] | None = None, *, project_root: Path | None = None) -> int:
    root = (project_root or Path(__file__).resolve().parents[1]).resolve()
    args = parse_args(argv)
    schedule = load_schedule(root)
    if args.job not in schedule["jobs"]:
        raise ValueError(f"unknown scheduled job: {args.job}")
    job = schedule["jobs"][args.job]
    commands = build_commands(project_root=root, job=job, verbose=args.verbose or args.dry_run)
    if args.dry_run:
        return 0

    settings = Settings.from_env(root)
    with advisory_lock(settings.database_url, schedule["lock_key"]):
        for command in commands:
            subprocess.run(command, cwd=root, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
