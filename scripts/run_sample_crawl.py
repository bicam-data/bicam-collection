from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib.util
import os
from pathlib import Path
import subprocess
import time

from bicam_collection.config import Settings
from bicam_collection.spec_loader import discover_spec_categories


def _load_run_crawl_main():
    run_crawl_path = Path(__file__).resolve().with_name("run_crawl.py")
    spec = importlib.util.spec_from_file_location("run_crawl", run_crawl_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {run_crawl_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.main


run_single_crawl_main = _load_run_crawl_main()


def parse_args(argv: list[str] | None = None, *, project_root: Path | None = None) -> argparse.Namespace:
    resolved_project_root = project_root or Path(__file__).resolve().parents[1]
    available_categories = discover_spec_categories(resolved_project_root / "specs")
    parser = argparse.ArgumentParser(description="Run a bounded multi-category Congress.gov crawl sample")
    parser.add_argument("--categories", nargs="*", choices=available_categories, default=available_categories)
    parser.add_argument("--mode", default="full", choices=["full", "incremental", "refresh", "resume", "refresh_descendants"])
    parser.add_argument("--max-pages", type=int, default=3)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--page-size", type=int, default=25)
    parser.add_argument("--from-datetime")
    parser.add_argument("--to-datetime")
    parser.add_argument("--async", dest="use_async", action="store_true")
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--category-workers", type=int, default=3)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dbt", choices=["skip", "run", "run-and-test"], default="skip")
    return parser.parse_args(argv)


def _run_dbt(project_root: Path, action: str) -> None:
    if action == "skip":
        return
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", str(project_root / "dbt_project"))
    commands: list[list[str]] = [["uv", "run", "dbt", "run", "--project-dir", "dbt_project"]]
    if action == "run-and-test":
        commands.append(["uv", "run", "dbt", "test", "--project-dir", "dbt_project"])
    for command in commands:
        started_at = time.perf_counter()
        subprocess.run(command, cwd=project_root, env=env, check=True)
        elapsed = time.perf_counter() - started_at
        print(f"completed {' '.join(command[2:])} in {elapsed:.1f}s")


def _build_category_argv(args: argparse.Namespace, *, category: str) -> list[str]:
    command_argv = [
        "--category",
        category,
        "--mode",
        args.mode,
        "--max-pages",
        str(args.max_pages),
    ]
    if args.batch_size is not None:
        command_argv.extend(["--batch-size", str(args.batch_size)])
    if args.page_size is not None:
        command_argv.extend(["--page-size", str(args.page_size)])
    if args.from_datetime:
        command_argv.extend(["--from-datetime", args.from_datetime])
    if args.to_datetime:
        command_argv.extend(["--to-datetime", args.to_datetime])
    if args.use_async:
        command_argv.extend(["--async", "--concurrency", str(args.concurrency)])
    if args.verbose:
        command_argv.append("--verbose")
    return command_argv


def _run_single_category(category: str, command_argv: list[str]) -> tuple[str, float]:
    started_at = time.perf_counter()
    exit_code = run_single_crawl_main(command_argv)
    if exit_code:
        raise RuntimeError(f"category {category} failed with exit code {exit_code}")
    return category, time.perf_counter() - started_at


def _run_categories(args: argparse.Namespace) -> None:
    max_workers = max(1, min(args.category_workers, len(args.categories)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_run_single_category, category, _build_category_argv(args, category=category)): category
            for category in args.categories
        }
        for future in as_completed(futures):
            category, elapsed = future.result()
            print(f"[sample] completed {category} in {elapsed:.1f}s")


def main(argv: list[str] | None = None) -> int:
    project_root = Path(__file__).resolve().parents[1]
    args = parse_args(argv, project_root=project_root)
    settings = Settings.from_env(project_root)
    os.environ.setdefault("DATABASE_URL", settings.database_url)

    started_at = time.perf_counter()
    _run_categories(args)
    _run_dbt(project_root, args.dbt)
    print(f"[sample] completed {len(args.categories)} categories in {time.perf_counter() - started_at:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
