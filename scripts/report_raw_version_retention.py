"""Measure, and optionally prune, superseded raw response versions.

``raw_response_versions`` keeps every distinct payload observed at an endpoint.
The current payload is the version whose ``response_hash`` matches the
``raw_responses`` row; every other version of that endpoint is superseded.
Superseded versions are still evidence: the fetch ledger references them by
foreign key, the Congress.gov child resolver can select them as part of the
newest complete generation, and release evidence bundles bind them by id.

The default mode is a read-only dry run.  It walks the version table in
bounded primary-key windows, classifies each superseded version, and prints a
JSON report with counts, estimated payload bytes, and sample candidate ids.

Retention classes, evaluated in order:

* ``kept_current``            the version is the current payload for its URL
* ``kept_newest_k``           within the newest ``--keep-versions`` of its URL
* ``kept_younger_than_d``     last seen within ``--older-than-days``
* ``kept_resolver_selected``  present in ``int_congress_child_response_selections``
* ``kept_ledger_recent``      referenced by a fetch within ``--ledger-window-days``
* ``kept_ledger_stale``       referenced only by older fetches
* ``prune_duplicate_payload`` prunable and its payload is still current elsewhere
* ``prune_remainder``         prunable, payload not retained anywhere else

``kept_ledger_stale`` is retained even though the policy window has expired:
the ledger foreign key would have to be nulled first, and
``scripts/backfill_fetch_version_links.py`` relinks null references to the
sole remaining version of an endpoint, which would invent lineage that never
happened.  This tool never modifies the fetch ledger.

``--apply`` deletes ``prune_*`` versions only when ``--confirm-delete`` is also
given.  Deletes run in bounded batches, one short transaction each, under a
statement timeout, and re-check that the version is still superseded at
delete time because crawl timers write concurrently.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

import psycopg

from bicam_collection.config import Settings


STATEMENT_TIMEOUT = "60s"
SAMPLE_IDS_PER_CLASS = 20
PRUNE_CLASSES = ("prune_duplicate_payload", "prune_remainder")
RETAINED_CLASSES = (
    "kept_current",
    "kept_newest_k",
    "kept_younger_than_d",
    "kept_resolver_selected",
    "kept_ledger_recent",
    "kept_ledger_stale",
)


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    keep_versions: int
    older_than_days: int
    ledger_window_days: int


@dataclass(frozen=True, slots=True)
class ClassifiedVersion:
    version_id: str
    retention_class: str
    payload_bytes: int


RESOLVER_SELECTED_PREDICATE = """exists (
            select 1
            from int_congress_child_response_selections as selected
            where selected.raw_response_version_id = window_versions.id
        )"""

# "Duplicate" means the exact payload is still the current payload of some
# endpoint, so deleting this copy loses no content.  Without an index whose
# leading column is response_hash this predicate is a sequential scan of the
# version heap per window, so the caller only enables it when the index exists.
DUPLICATE_OF_CURRENT_PREDICATE = """exists (
            select 1
            from raw_response_versions as other
            join raw_responses as other_current
              on other_current.id = other.raw_response_id
            where other.response_hash = window_versions.response_hash
              and other.id <> window_versions.id
              and other_current.response_hash = other.response_hash
        )"""

RESPONSE_HASH_INDEX_SQL = """
select exists (
    select 1
    from pg_index as index_entry
    join pg_class as table_class on table_class.oid = index_entry.indrelid
    join pg_attribute as leading_column
      on leading_column.attrelid = table_class.oid
     and leading_column.attnum = index_entry.indkey[0]
    where table_class.relname = 'raw_response_versions'
      and leading_column.attname = 'response_hash'
)
"""

RESOLVER_TABLE_SQL = "select to_regclass('int_congress_child_response_selections') is not null"

ESTIMATED_VERSION_COUNT_SQL = """
select reltuples::bigint
from pg_class
where oid = 'raw_response_versions'::regclass
"""

DELETE_SQL = """
delete from raw_response_versions as versions
using raw_responses as current_response
where versions.id = any(%(version_ids)s::uuid[])
  and current_response.id = versions.raw_response_id
  and current_response.response_hash <> versions.response_hash
"""


def classification_sql(*, resolver_table_present: bool, response_hash_index_present: bool) -> str:
    resolver_predicate = RESOLVER_SELECTED_PREDICATE if resolver_table_present else "false"
    duplicate_predicate = DUPLICATE_OF_CURRENT_PREDICATE if response_hash_index_present else "false"
    # The CASE keeps the correlated subqueries lazy: PostgreSQL only runs a
    # SubPlan when its branch is reached, so current versions (the vast
    # majority) cost one primary-key lookup and nothing else.
    return f"""
with window_versions as (
    select
        versions.id,
        versions.raw_response_id,
        versions.response_hash,
        versions.last_seen_at,
        pg_column_size(versions.response) as payload_bytes,
        current_response.response_hash = versions.response_hash as is_current
    from raw_response_versions as versions
    join raw_responses as current_response
      on current_response.id = versions.raw_response_id
    where versions.id > %(after_id)s::uuid
    order by versions.id
    limit %(batch_size)s
)
select
    window_versions.id,
    window_versions.payload_bytes,
    case
        when window_versions.is_current then 'kept_current'
        when (
            select count(*)
            from raw_response_versions as newer
            where newer.raw_response_id = window_versions.raw_response_id
              and (newer.last_seen_at, newer.id) > (window_versions.last_seen_at, window_versions.id)
        ) < %(keep_versions)s then 'kept_newest_k'
        when window_versions.last_seen_at >= now() - make_interval(days => %(older_than_days)s)
            then 'kept_younger_than_d'
        when {resolver_predicate} then 'kept_resolver_selected'
        when exists (
            select 1
            from crawl_run_fetches as fetches
            where fetches.raw_response_version_id = window_versions.id
              and fetches.fetched_at >= now() - make_interval(days => %(ledger_window_days)s)
        ) then 'kept_ledger_recent'
        when exists (
            select 1
            from crawl_run_fetches as fetches
            where fetches.raw_response_version_id = window_versions.id
        ) then 'kept_ledger_stale'
        when {duplicate_predicate} then 'prune_duplicate_payload'
        else 'prune_remainder'
    end as retention_class
from window_versions
order by window_versions.id
"""


def classify_window(
    connection: Any,
    sql: str,
    policy: RetentionPolicy,
    *,
    after_id: str,
    batch_size: int,
) -> list[ClassifiedVersion]:
    rows = connection.execute(
        sql,
        {
            "after_id": after_id,
            "batch_size": batch_size,
            "keep_versions": policy.keep_versions,
            "older_than_days": policy.older_than_days,
            "ledger_window_days": policy.ledger_window_days,
        },
    ).fetchall()
    return [ClassifiedVersion(str(row[0]), row[2], int(row[1])) for row in rows]


def delete_versions(connection: Any, version_ids: list[str]) -> int:
    """Delete one batch in its own transaction and return the rows removed."""

    try:
        deleted = connection.execute(DELETE_SQL, {"version_ids": version_ids}).rowcount
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return int(deleted)


def run_report(
    connection: Any,
    policy: RetentionPolicy,
    *,
    batch_size: int,
    limit: int,
    apply: bool,
) -> dict[str, Any]:
    resolver_table_present = bool(connection.execute(RESOLVER_TABLE_SQL).fetchone()[0])
    response_hash_index_present = bool(connection.execute(RESPONSE_HASH_INDEX_SQL).fetchone()[0])
    estimated_total = int(connection.execute(ESTIMATED_VERSION_COUNT_SQL).fetchone()[0])
    sql = classification_sql(
        resolver_table_present=resolver_table_present,
        response_hash_index_present=response_hash_index_present,
    )

    counts = {name: 0 for name in RETAINED_CLASSES + PRUNE_CLASSES}
    payload_bytes = {name: 0 for name in RETAINED_CLASSES + PRUNE_CLASSES}
    sample_ids: dict[str, list[str]] = {name: [] for name in PRUNE_CLASSES}
    examined = 0
    batches = 0
    deleted = 0
    after_id = "00000000-0000-0000-0000-000000000000"
    scan_complete = False

    while True:
        window_size = min(batch_size, limit - examined) if limit else batch_size
        window = classify_window(connection, sql, policy, after_id=after_id, batch_size=window_size)
        if not window:
            scan_complete = True
            break
        # Each window is a read-only transaction of its own so the dry run
        # never holds a snapshot open across the whole walk.
        connection.commit()
        batches += 1
        examined += len(window)
        after_id = window[-1].version_id
        prunable_ids: list[str] = []
        for version in window:
            counts[version.retention_class] += 1
            payload_bytes[version.retention_class] += version.payload_bytes
            if version.retention_class in PRUNE_CLASSES:
                prunable_ids.append(version.version_id)
                if len(sample_ids[version.retention_class]) < SAMPLE_IDS_PER_CLASS:
                    sample_ids[version.retention_class].append(version.version_id)
        batch_deleted = 0
        if apply and prunable_ids:
            batch_deleted = delete_versions(connection, prunable_ids)
            deleted += batch_deleted
        print(
            f"batch={batches} examined={examined} prunable_in_batch={len(prunable_ids)} "
            f"deleted_in_batch={batch_deleted} last_id={after_id}",
            file=sys.stderr,
        )
        if len(window) < window_size:
            scan_complete = True
            break
        if limit and examined >= limit:
            break

    prunable_versions = sum(counts[name] for name in PRUNE_CLASSES)
    prunable_bytes = sum(payload_bytes[name] for name in PRUNE_CLASSES)
    scale = (estimated_total / examined) if examined and not scan_complete else 1.0
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "apply" if apply else "dry-run",
        "policy": {
            "keep_versions": policy.keep_versions,
            "older_than_days": policy.older_than_days,
            "ledger_window_days": policy.ledger_window_days,
        },
        "scan": {
            "batch_size": batch_size,
            "limit": limit,
            "batches": batches,
            "versions_examined": examined,
            "versions_estimated_total": estimated_total,
            "complete": scan_complete,
            "last_version_id": after_id,
        },
        "resolver_selection_table_present": resolver_table_present,
        "response_hash_index_present": response_hash_index_present,
        "classes": {
            name: {"versions": counts[name], "payload_bytes": payload_bytes[name]}
            for name in RETAINED_CLASSES + PRUNE_CLASSES
        },
        "prunable": {
            "versions": prunable_versions,
            "payload_bytes": prunable_bytes,
            "extrapolated_versions": round(prunable_versions * scale),
            "extrapolated_payload_bytes": round(prunable_bytes * scale),
        },
        "sample_candidate_ids": sample_ids,
        "deleted_versions": deleted,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--keep-versions", type=int, default=2, help="Newest versions kept per endpoint, including the current one (default: 2).")
    parser.add_argument("--older-than-days", type=int, default=180, help="Only versions last seen more than this many days ago are prunable (default: 180).")
    parser.add_argument("--ledger-window-days", type=int, default=90, help="Fetch-ledger references newer than this keep a version as kept_ledger_recent (default: 90).")
    parser.add_argument("--batch-size", type=int, default=20_000, help="Version rows examined per query and maximum deletes per transaction (default: 20000).")
    parser.add_argument("--limit", type=int, default=100_000, help="Maximum version rows examined; 0 walks the whole table (default: 100000).")
    parser.add_argument("--apply", action="store_true", help="Delete prunable versions. Refused unless --confirm-delete is also given.")
    parser.add_argument("--confirm-delete", action="store_true", help="Second interlock for --apply.")
    args = parser.parse_args(argv)
    if args.keep_versions < 1:
        parser.error("--keep-versions must be at least 1 so the current version is always kept")
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    if args.limit < 0:
        parser.error("--limit must be zero or positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.apply and not args.confirm_delete:
        print("refusing --apply without --confirm-delete; no connection was opened", file=sys.stderr)
        return 2
    policy = RetentionPolicy(
        keep_versions=args.keep_versions,
        older_than_days=args.older_than_days,
        ledger_window_days=args.ledger_window_days,
    )
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    with psycopg.connect(settings.database_url) as connection:
        connection.execute(f"set statement_timeout = '{STATEMENT_TIMEOUT}'")
        if not args.apply:
            connection.execute("set default_transaction_read_only = on")
        connection.commit()
        report = run_report(
            connection,
            policy,
            batch_size=args.batch_size,
            limit=args.limit,
            apply=args.apply,
        )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
