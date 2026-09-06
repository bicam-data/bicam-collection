#!/usr/bin/env python3
"""Benchmark deterministic bill-title selection on an isolated PostgreSQL DB.

The workload is deliberately transaction-local: it creates temporary tables,
loads a representative number of repeated title observations, runs the same
window-ordering shape used by ``bicam.bills_titles``, and leaves no persistent
database objects behind.  This makes the Package-B performance gate repeatable
without requiring production-scale source data or a production connection.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

import psycopg


DEFAULT_BILL_COUNT = 100_000
DEFAULT_OBSERVATIONS_PER_BILL = 3
DEFAULT_THRESHOLD_MS = 5_000.0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Disposable PostgreSQL DSN (defaults to DATABASE_URL).",
    )
    parser.add_argument("--bill-count", type=int, default=DEFAULT_BILL_COUNT)
    parser.add_argument(
        "--observations-per-bill",
        type=int,
        default=DEFAULT_OBSERVATIONS_PER_BILL,
    )
    parser.add_argument("--threshold-ms", type=float, default=DEFAULT_THRESHOLD_MS)
    parser.add_argument("--output", type=Path, help="Optional JSON artifact path.")
    return parser.parse_args(argv)


def _validate_args(args: argparse.Namespace) -> None:
    if not args.database_url:
        raise SystemExit("--database-url or DATABASE_URL is required")
    if args.bill_count < 1:
        raise SystemExit("--bill-count must be positive")
    if args.observations_per_bill < 2:
        raise SystemExit("--observations-per-bill must be at least 2")
    if args.threshold_ms <= 0:
        raise SystemExit("--threshold-ms must be positive")


def _selection_sql() -> str:
    return """
        with title_observations as (
            select
                t.*,
                row_number() over (
                    partition by t.bill_id, t.title_type, t.title
                    order by (nullif(trim(t.bill_text_version_code), '') is not null) desc,
                             (nullif(trim(t.bill_text_version_name), '') is not null) desc,
                             t.updated_at desc nulls last,
                             nullif(trim(t.bill_text_version_code), '') desc nulls last,
                             nullif(trim(t.bill_text_version_name), '') desc nulls last,
                             t.chamber desc nulls last,
                             t.title_type_code desc nulls last
                ) as _rn
            from bench_title_observations as t
            join bench_bills as b using (bill_id)
        )
        select count(*)::bigint
        from title_observations
        where _rn = 1
    """


def _plan_metrics(plan: Any) -> dict[str, int]:
    metrics = {"sort_nodes": 0, "sort_space_disk_kb": 0}

    def visit(node: Any) -> None:
        if not isinstance(node, dict):
            return
        if node.get("Node Type") in {"Sort", "Incremental Sort"}:
            metrics["sort_nodes"] += 1
            metrics["sort_space_disk_kb"] += int(node.get("Sort Space Used", 0) or 0)
        for child in node.get("Plans", []):
            visit(child)

    visit(plan)
    return metrics


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    _validate_args(args)
    with psycopg.connect(args.database_url) as connection:
        connection.execute("set work_mem = '64MB'")
        connection.execute(
            """
            create temp table bench_bills (
                bill_id text primary key
            ) on commit drop
            """
        )
        connection.execute(
            """
            insert into bench_bills (bill_id)
            select 'benchmark-' || value::text
            from generate_series(1, %s) as values(value)
            """,
            (args.bill_count,),
        )
        connection.execute(
            """
            create temp table bench_title_observations (
                bill_id text not null,
                title text not null,
                title_type text not null,
                chamber text,
                title_type_code text,
                bill_text_version_name text,
                bill_text_version_code text,
                updated_at timestamptz
            ) on commit drop
            """
        )
        connection.execute(
            """
            insert into bench_title_observations (
                bill_id, title, title_type, chamber, title_type_code,
                bill_text_version_name, bill_text_version_code, updated_at
            )
            select
                b.bill_id,
                'Benchmark title ' || b.bill_id,
                'Official Title as Introduced',
                'house',
                'OL title',
                case when observation = %s then 'Introduced in House' end,
                case when observation = %s then 'IH' end,
                timestamptz '2023-01-01 00:00:00+00' +
                    (observation * interval '1 day')
            from bench_bills as b
            cross join generate_series(1, %s) as values(observation)
            """,
            (
                args.observations_per_bill,
                args.observations_per_bill,
                args.observations_per_bill,
            ),
        )
        connection.execute("analyze bench_bills")
        connection.execute("analyze bench_title_observations")
        explain = connection.execute(
            "explain (analyze, buffers, format json) " + _selection_sql()
        ).fetchone()[0][0]
        execution_ms = float(explain["Execution Time"])
        planning_ms = float(explain["Planning Time"])
        connection.commit()

    title_rows = args.bill_count * args.observations_per_bill
    return {
        "benchmark": "bill_title_selection",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bill_count": args.bill_count,
        "observations_per_bill": args.observations_per_bill,
        "title_observation_rows": title_rows,
        "threshold_ms": args.threshold_ms,
        "planning_ms": planning_ms,
        "execution_ms": execution_ms,
        "passed": execution_ms <= args.threshold_ms,
        "plan_metrics": _plan_metrics(explain["Plan"]),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_benchmark(args)
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
