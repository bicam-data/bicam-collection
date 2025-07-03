"""Light-weight CLI replacing the monolithic *main.py*.

Usage examples:
    python -m bicam_collection.dagster_pipeline.cli process bills
    python -m bicam_collection.dagster_pipeline.cli process bills --phases raw staging
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

from ..libs.app_context import ApplicationContext
from ..libs.config import BicamConfig
from .pipeline_runner import run_single_data_type

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))
logger = logging.getLogger("bicam.cli")


def _build_context() -> ApplicationContext:
    cfg = BicamConfig.from_env()
    logger.debug("Loaded configuration: %s", cfg)
    return ApplicationContext(cfg)


def _cmd_process(args: argparse.Namespace):
    ctx = _build_context()
    try:
        run_single_data_type(ctx, args.data_type, args.phases)
    finally:
        # ensure we close resources even on exception
        asyncio.run(ctx.shutdown())


def _create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("bicam-collection data pipeline")
    sub = p.add_subparsers(dest="command", required=True)

    # ------------------------------------------------------------------
    # process
    proc = sub.add_parser("process", help="Run pipeline for one data-type")
    proc.add_argument("data_type")
    proc.add_argument(
        "--phases",
        nargs="+",
        choices=["raw", "staging", "production"],
        help="Subset of phases to run",
    )

    # ------------------------------------------------------------------
    # setup-db
    setup_p = sub.add_parser("setup-db", help="Create database schemas/tables")
    setup_p.add_argument("--recreate", action="store_true", help="Drop & recreate")

    # ------------------------------------------------------------------
    # check-dates
    sub.add_parser("check-dates", help="Print last_processed dates")

    # reset-dates
    reset_p = sub.add_parser("reset-dates", help="Delete last_processed entries")
    reset_p.add_argument(
        "--data-types", nargs="+", help="Subset of data-types (defaults all)"
    )

    # clear-checkpoints
    cp = sub.add_parser(
        "clear-checkpoints", help="Delete SQLite checkpoints for data-type"
    )
    cp.add_argument("data_type")

    # list-types
    sub.add_parser("list-types", help="List registered data-types")

    return p


def main(argv: list[str] | None = None):
    parser = _create_parser()
    ns = parser.parse_args(argv)

    if ns.command == "process":
        _cmd_process(ns)
    elif ns.command == "setup-db":
        from .commands import setup_db

        ctx = _build_context()
        setup_db(ctx, recreate=ns.recreate)
    elif ns.command == "check-dates":
        from .commands import check_dates

        ctx = _build_context()
        check_dates(ctx)
    elif ns.command == "reset-dates":
        from .commands import reset_dates

        ctx = _build_context()
        reset_dates(ctx, ns.data_types)
    elif ns.command == "clear-checkpoints":
        from .commands import clear_checkpoints

        ctx = _build_context()
        clear_checkpoints(ctx, ns.data_type)
    elif ns.command == "list-types":
        from .commands import list_types

        list_types()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
