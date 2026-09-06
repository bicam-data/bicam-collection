from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

from bicam_collection.config import Settings
from bicam_collection.target_scope import (
    TARGET_CONGRESS_CATEGORIES,
    TARGET_RECONCILIATION_ENTITIES,
)

try:  # Support both ``python scripts/...`` and package imports in tests.
    from .check_child_coverage import run as run_child_coverage
except ImportError:  # pragma: no cover - exercised by direct script execution
    from check_child_coverage import run as run_child_coverage


TARGET_ROOT_ITEM_SPECS = (
    ("bills", "bill_list", "bills"),
    ("amendments", "amendment_list", "amendments"),
    ("committee-meetings", "committee_meeting_list", "committeeMeetings"),
    ("committee-prints", "committee_print_list", "committeePrints"),
    ("committee-reports", "committee_report_list", "reports"),
    ("committees", "committee_list", "committees"),
    ("hearings", "hearing_list", "hearings"),
    ("nominations", "nomination_list", "nominations"),
    ("treaties", "treaty_list", "treaties"),
    ("members", "member_list", "members"),
    ("congresses", "congress_list", "congresses"),
)

# Partition presence must be proved by a root-list response itself.  Checking
# for any response in the partition is insufficient: a detail response could
# make a partition look observed even when its list crawl never ran.
TARGET_ROOT_PARTITION_SPECS = (
    ("bills", "bill_list", "bill"),
    ("amendments", "amendment_list", "amendment"),
    ("committee-meetings", "committee_meeting_list", "committee-meeting"),
    ("committee-prints", "committee_print_list", "committee-print"),
    ("committee-reports", "committee_report_list", "committee-report"),
    ("committees", "committee_list", "committee"),
    ("hearings", "hearing_list", "hearing"),
    ("nominations", "nomination_list", "nomination"),
    ("treaties", "treaty_list", "treaty"),
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check crawler, coverage, and reconciliation quality gates.")
    parser.add_argument("--max-stale-hours", type=float, default=24.0)
    parser.add_argument("--max-open-exceptions", type=int, default=0)
    parser.add_argument("--max-unsatisfied-fetches", type=int, default=0)
    parser.add_argument("--max-missing-full-scope-partitions", type=int, default=0)
    parser.add_argument("--max-missing-root-items", type=int, default=0)
    parser.add_argument("--max-missing-child-items", type=int, default=0)
    return parser.parse_args(argv)


def run_checks(
    *,
    database_url: str,
    max_stale_hours: float,
    max_open_exceptions: int,
    max_unsatisfied_fetches: int,
    max_missing_full_scope_partitions: int = 0,
    max_missing_root_items: int = 0,
    max_missing_child_items: int = 0,
    project_root: Path | None = None,
) -> list[str]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_stale_hours)
    issues: list[str] = []
    with psycopg.connect(database_url) as conn:
        # Quality checks are operational diagnostics, not unbounded analytical
        # jobs.  A cancelled audit must not leave a PostgreSQL backend chewing
        # on a full raw-archive scan for hours after its shell timeout.
        conn.execute("set statement_timeout='240s'")
        conn.execute("set lock_timeout='15s'")
        stale_runs = conn.execute(
            """
            select count(*)
            from crawl_runs
            where status = 'running'
              and started_at < %s
              and category = any(%s)
            """,
            (cutoff, list(TARGET_CONGRESS_CATEGORIES)),
        ).fetchone()[0]
        if stale_runs > 0:
            issues.append(f"stale_running_runs={stale_runs}")

        failed_metrics = conn.execute(
            """
            select coalesce(string_agg(entity || ':' || quality_status, ',' order by entity), '')
            from fct_reconciliation_metrics
            where quality_status <> 'pass'
              and entity = any(%s)
            """,
            (list(TARGET_RECONCILIATION_ENTITIES),),
        ).fetchone()[0]
        if failed_metrics:
            issues.append(f"failed_reconciliation_metrics={failed_metrics}")

        open_exceptions = conn.execute(
            """
            select count(*)
            from fct_reconciliation_exceptions
            where exception_status = 'open'
              and entity = any(%s)
            """,
            (list(TARGET_RECONCILIATION_ENTITIES),),
        ).fetchone()[0]
        if open_exceptions > max_open_exceptions:
            issues.append(f"open_reconciliation_exceptions={open_exceptions}")

        unsatisfied_fetches = conn.execute(
            """
            select coalesce(sum(unsatisfied_fetch_count), 0)
            from fct_collection_coverage
            where category = any(%s)
            """,
            (list(TARGET_CONGRESS_CATEGORIES),),
        ).fetchone()[0]
        if unsatisfied_fetches > max_unsatisfied_fetches:
            issues.append(f"unsatisfied_fetches={unsatisfied_fetches}")

        # Require a current root-list response for every declared partition,
        # rather than accepting a detail-only response as evidence that the
        # partition was crawled.  Empty HTTP-200 root results are observed and
        # therefore satisfy presence; the exact response remains in the raw
        # archive for source-availability review.
        root_partition_values = ",\n                ".join(
            "(%s, %s, %s)" for _ in TARGET_ROOT_PARTITION_SPECS
        )
        root_partition_params: list[str] = []
        for category, node_type, path in TARGET_ROOT_PARTITION_SPECS:
            root_partition_params.extend((category, node_type, path))
        missing_full_scope = conn.execute(
            f"""
            with specs(category, node_type, path) as (
                values {root_partition_values}
            ), scope as (
                select
                    partitions.category,
                    regexp_replace(trim(partition_value), '^p', '') as partition_value,
                    specs.node_type,
                    specs.path
                from collection_scope_partitions as partitions
                join specs using (category)
                cross join lateral regexp_split_to_table(
                    partitions.partition_values, ','
                ) as split(partition_value)
                where partitions.source = 'congress'
                  and partitions.partition_field = 'congress'
                  and partitions.category = any(%s)
            ), observed as (
                select distinct
                    scope.category,
                    scope.partition_value
                from scope
                join raw_responses as responses
                  on responses.source = 'congress'
                 and responses.category = scope.category
                 and responses.node_type = scope.node_type
                 and responses.endpoint_url ~ (
                     '/'
                     || scope.path
                     || '/'
                     || scope.partition_value
                     || '(?:[?/]|$)'
                 )
            )
            select count(*)
            from scope
            left join observed using (category, partition_value)
            where observed.category is null
            """,
            (*root_partition_params, list(TARGET_CONGRESS_CATEGORIES)),
        ).fetchone()[0]
        if missing_full_scope > max_missing_full_scope_partitions:
            issues.append(f"missing_full_scope_partitions={missing_full_scope}")

        # A partition can have observed data while still missing a large
        # fraction of its root items' detail responses.  Compare the URLs
        # enumerated by each Congress.gov list response with the immutable
        # response archive so this gate catches partial child crawls (the
        # prior partition-presence check did not).
        root_item_specs = TARGET_ROOT_ITEM_SPECS
        # Build the two large URL sets once.  The previous per-category query
        # repeatedly scanned and de-duplicated the entire raw archive; with
        # millions of stored responses, overlapping audit runs could leave
        # several PostgreSQL workers consuming CPU long after the audit shell
        # had timed out.
        conn.execute("set max_parallel_workers_per_gather=0")
        conn.execute(
            """
            create temporary table quality_gate_response_paths as
            select distinct regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
            from raw_responses
            where source = 'congress'
            """
        )
        conn.execute(
            """
            create index quality_gate_response_paths_idx
            on quality_gate_response_paths(endpoint_path)
            """
        )
        conn.execute("analyze quality_gate_response_paths")
        conn.execute(
            """
            create temporary table quality_gate_root_items as
            select distinct
                specs.category,
                specs.node_type,
                regexp_replace(item->>'url', '[?].*$', '') as endpoint_path
            from (values
                ('bills', 'bill_list', 'bills'),
                ('amendments', 'amendment_list', 'amendments'),
                ('committee-meetings', 'committee_meeting_list', 'committeeMeetings'),
                ('committee-prints', 'committee_print_list', 'committeePrints'),
                ('committee-reports', 'committee_report_list', 'reports'),
                ('committees', 'committee_list', 'committees'),
                ('hearings', 'hearing_list', 'hearings'),
                ('nominations', 'nomination_list', 'nominations'),
                ('treaties', 'treaty_list', 'treaties'),
                ('members', 'member_list', 'members'),
                ('congresses', 'congress_list', 'congresses')
            ) as specs(category, node_type, item_key)
            join raw_responses as lists
              on lists.source = 'congress'
             and lists.category = specs.category
             and lists.node_type = specs.node_type
            cross join lateral jsonb_array_elements(
                coalesce(lists.response -> specs.item_key, '[]'::jsonb)
            ) as item
            where item->>'url' is not null

            """
        )
        conn.execute(
            """
            create index quality_gate_root_items_idx
            on quality_gate_root_items(category, node_type, endpoint_path)
            """
        )
        conn.execute("analyze quality_gate_root_items")
        conn.commit()
        # A detail endpoint can remain absent because Congress.gov itself
        # returns a durable source-side 404/500 (for example historical
        # ``MemberTerm DoesNotExist`` payloads).  Those URLs must remain in
        # the failure ledger, but they are not actionable crawler omissions
        # and should not cause every scheduled quality audit to reclassify the
        # same source defect as an ingestion regression.  Match on the stable
        # path because older failure rows may differ only in query ordering.
        conn.execute(
            """
            create temporary table quality_gate_root_source_errors as
            select
                category,
                regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
            from failed_requests
            where http_status in (404, 500)
            group by category, regexp_replace(endpoint_url, '[?].*$', '')
            having count(*) >= 3
            """
        )
        conn.execute(
            """
            create index quality_gate_root_source_errors_idx
            on quality_gate_root_source_errors(category, endpoint_path)
            """
        )
        conn.execute("analyze quality_gate_root_source_errors")
        missing_root_by_category: list[str] = []
        for category, node_type, item_key in root_item_specs:
            row = conn.execute(
                """
                select count(*) filter (
                    where paths.endpoint_path is null
                      and source_errors.endpoint_path is null
                )
                from quality_gate_root_items as items
                left join quality_gate_response_paths as paths using (endpoint_path)
                left join quality_gate_root_source_errors as source_errors
                  using (category, endpoint_path)
                where items.category = %s
                  and items.node_type = %s
                """,
                (category, node_type),
            ).fetchone()
            missing = int(row[0] or 0)
            if missing:
                missing_root_by_category.append(f"{category}={missing}")
        missing_root_items = sum(
            int(entry.rsplit("=", 1)[1]) for entry in missing_root_by_category
        )
        if missing_root_items > max_missing_root_items:
            issues.append(
                "missing_root_items="
                + str(missing_root_items)
                + (" (" + ",".join(missing_root_by_category) + ")" if missing_root_by_category else "")
            )
    child_results = run_child_coverage(
        database_url=database_url,
        project_root=project_root or Path(__file__).resolve().parents[1],
    )
    missing_child_items = sum(int(result["missing"]) for result in child_results)
    if missing_child_items > max_missing_child_items:
        issues.append(f"missing_child_items={missing_child_items}")
    return issues


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    issues = run_checks(
        database_url=settings.database_url,
        max_stale_hours=args.max_stale_hours,
        max_open_exceptions=args.max_open_exceptions,
        max_unsatisfied_fetches=args.max_unsatisfied_fetches,
        max_missing_full_scope_partitions=args.max_missing_full_scope_partitions,
        max_missing_root_items=args.max_missing_root_items,
        max_missing_child_items=args.max_missing_child_items,
        project_root=project_root,
    )
    if issues:
        print("QUALITY GATE FAILED: " + "; ".join(issues))
        return 1
    print("QUALITY GATE PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
