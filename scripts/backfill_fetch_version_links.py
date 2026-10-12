"""Backfill raw_response_version_id on legacy skipped-fetch rows, in bounded batches.

Older crawls recorded only raw_response_id for skipped URLs. A response with one
retained version is unambiguous; the opt-in temporal pass picks the version
current at the event time, rejecting ties. Safe to stop and rerun.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import time

import psycopg

from bicam_collection.config import Settings


INDEX_NAME = "idx_crawl_run_fetches_missing_raw_response_id"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill exact version IDs for legacy skipped-existing fetches."
    )
    parser.add_argument(
        "--batch-raw-ids",
        type=int,
        default=5_000,
        help="Raw response IDs updated per transaction (default: 5000).",
    )
    parser.add_argument(
        "--max-fetch-rows",
        type=int,
        default=1_000_000,
        help="Maximum fetch rows to update in this invocation (default: 1000000).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all currently repairable rows instead of the bounded default.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report repairable rows without creating an index or updating rows.",
    )
    parser.add_argument(
        "--skip-count",
        action="store_true",
        help="Skip the full repairable-row count before starting a batch.",
    )
    parser.add_argument(
        "--resolve-temporal",
        action="store_true",
        help=(
            "Also resolve multi-version endpoints by choosing the unique payload "
            "with the latest first_seen_at not after the fetch event."
        ),
    )
    parser.add_argument(
        "--resolve-endpoint",
        action="store_true",
        help=(
            "Resolve legacy skipped-existing rows with no raw ID by matching "
            "their endpoint and event time to an immutable payload."
        ),
    )
    return parser.parse_args(argv)


def ensure_lookup_index(connection: psycopg.Connection) -> None:
    """Create the partial lookup index without blocking crawler writes."""

    connection.execute(
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME} "
        "ON crawl_run_fetches(raw_response_id) "
        "WHERE raw_response_id IS NOT NULL "
        "AND raw_response_version_id IS NULL "
        "AND outcome = 'skipped_existing_backfill'"
    )


def create_singleton_map(connection: psycopg.Connection) -> None:
    """Materialize the unambiguous raw-response→version mapping once."""

    connection.execute("DROP TABLE IF EXISTS _fetch_lineage_singletons")
    connection.execute(
        """
        CREATE TEMP TABLE _fetch_lineage_singletons ON COMMIT PRESERVE ROWS AS
        SELECT raw_response_id,
               (array_agg(id ORDER BY id))[1] AS raw_response_version_id
        FROM raw_response_versions
        GROUP BY raw_response_id
        HAVING count(*) = 1
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX _fetch_lineage_singletons_raw_id_idx
            ON _fetch_lineage_singletons(raw_response_id)
        """
    )
    connection.execute("ANALYZE _fetch_lineage_singletons")


def repairable_count(connection: psycopg.Connection) -> int:
    row = connection.execute(
        """
        SELECT count(*)
        FROM crawl_run_fetches AS fetches
        JOIN _fetch_lineage_singletons AS singletons
          ON singletons.raw_response_id = fetches.raw_response_id
        WHERE fetches.raw_response_version_id IS NULL
          AND fetches.outcome = 'skipped_existing_backfill'
        """
    ).fetchone()
    return int(row[0] if row else 0)


def null_version_summary(connection: psycopg.Connection) -> list[tuple[str, int, int]]:
    """Split NULL-version fetch rows into the repairable backlog and rows that never
    have a payload (failures, same-run duplicates).
    """

    rows = connection.execute(
        """
        SELECT outcome,
               count(*) AS total,
               count(*) FILTER (WHERE raw_response_id IS NOT NULL) AS with_raw_id
        FROM crawl_run_fetches
        WHERE raw_response_version_id IS NULL
        GROUP BY outcome
        ORDER BY outcome
        """
    ).fetchall()
    return [(str(row[0]), int(row[1]), int(row[2])) for row in rows]


def create_endpoint_candidates(connection: psycopg.Connection) -> None:
    """Match legacy rows lacking even a raw_response_id by endpoint and event time,
    rejecting same-timestamp ties.
    """

    connection.execute("DROP TABLE IF EXISTS _fetch_lineage_endpoint_candidates")
    connection.execute(
        """
        CREATE TEMP TABLE _fetch_lineage_endpoint_candidates
            ON COMMIT PRESERVE ROWS AS
        SELECT fetches.id AS fetch_id,
               raws.id AS raw_response_id,
               chosen.raw_response_version_id
        FROM crawl_run_fetches AS fetches
        JOIN raw_responses AS raws
          ON raws.endpoint_url = fetches.endpoint_url
        CROSS JOIN LATERAL (
            SELECT versions.id AS raw_response_version_id,
                   versions.first_seen_at
            FROM raw_response_versions AS versions
            WHERE versions.raw_response_id = raws.id
              AND versions.first_seen_at <= fetches.fetched_at
            ORDER BY versions.first_seen_at DESC, versions.id DESC
            LIMIT 1
        ) AS chosen
        WHERE fetches.raw_response_id IS NULL
          AND fetches.raw_response_version_id IS NULL
          AND fetches.outcome = 'skipped_existing'
          AND NOT EXISTS (
              SELECT 1
              FROM raw_response_versions AS tie
              WHERE tie.raw_response_id = raws.id
                AND tie.first_seen_at = chosen.first_seen_at
                AND tie.id <> chosen.raw_response_version_id
          )
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX _fetch_lineage_endpoint_candidates_fetch_id_idx
            ON _fetch_lineage_endpoint_candidates(fetch_id)
        """
    )
    connection.execute("ANALYZE _fetch_lineage_endpoint_candidates")


def endpoint_candidate_count(connection: psycopg.Connection) -> int:
    row = connection.execute(
        "SELECT count(*) FROM _fetch_lineage_endpoint_candidates"
    ).fetchone()
    return int(row[0] if row else 0)


def repair_endpoint(
    connection: psycopg.Connection,
    *,
    batch_rows: int,
    max_fetch_rows: int | None,
) -> int:
    """Apply endpoint/time matches in bounded transactions."""

    updated_total = 0
    last_fetch_id = "00000000-0000-0000-0000-000000000000"
    while max_fetch_rows is None or updated_total < max_fetch_rows:
        limit = max(
            1,
            min(batch_rows, max_fetch_rows - updated_total)
            if max_fetch_rows is not None
            else batch_rows,
        )
        with connection.transaction():
            batch = connection.execute(
                """
                SELECT fetch_id
                FROM _fetch_lineage_endpoint_candidates
                WHERE fetch_id > %s::uuid
                ORDER BY fetch_id
                LIMIT %s
                """,
                (last_fetch_id, limit),
            ).fetchall()
            if not batch:
                return updated_total
            result = connection.execute(
                """
                UPDATE crawl_run_fetches AS fetches
                SET raw_response_id = candidates.raw_response_id,
                    raw_response_version_id = candidates.raw_response_version_id
                FROM _fetch_lineage_endpoint_candidates AS candidates
                WHERE candidates.fetch_id = fetches.id
                  AND fetches.raw_response_id IS NULL
                  AND fetches.raw_response_version_id IS NULL
                  AND fetches.outcome = 'skipped_existing'
                  AND candidates.fetch_id = ANY(%s::uuid[])
                """,
                ([str(row[0]) for row in batch],),
            )
            updated = int(result.rowcount or 0)
        updated_total += updated
        last_fetch_id = str(batch[-1][0])
        print(
            f"[lineage-endpoint] rows={updated} total={updated_total}",
            flush=True,
        )
    return updated_total


def repair(
    connection: psycopg.Connection,
    *,
    batch_raw_ids: int,
    max_fetch_rows: int | None,
) -> int:
    """Update singleton-version rows, advancing in UUID order."""

    updated_total = 0
    last_raw_response_id = "00000000-0000-0000-0000-000000000000"
    while max_fetch_rows is None or updated_total < max_fetch_rows:
        raw_id_limit = max(1, batch_raw_ids)
        with connection.transaction():
            batch = connection.execute(
                """
                SELECT raw_response_id, raw_response_version_id
                FROM _fetch_lineage_singletons
                WHERE raw_response_id > %s::uuid
                  AND EXISTS (
                      SELECT 1
                      FROM crawl_run_fetches AS pending
                      WHERE pending.raw_response_id = _fetch_lineage_singletons.raw_response_id
                        AND pending.raw_response_version_id IS NULL
                        AND pending.outcome = 'skipped_existing_backfill'
                  )
                ORDER BY raw_response_id
                LIMIT %s
                """,
                (last_raw_response_id, raw_id_limit),
            ).fetchall()
            if not batch:
                return updated_total
            result = connection.execute(
                """
                UPDATE crawl_run_fetches AS fetches
                SET raw_response_version_id = singletons.raw_response_version_id
                FROM _fetch_lineage_singletons AS singletons
                WHERE singletons.raw_response_id = fetches.raw_response_id
                  AND fetches.raw_response_id >= %s::uuid
                  AND fetches.raw_response_id <= %s::uuid
                  AND fetches.raw_response_version_id IS NULL
                  AND fetches.outcome = 'skipped_existing_backfill'
                """,
                (batch[0][0], batch[-1][0]),
            )
            updated = int(result.rowcount or 0)
        updated_total += updated
        last_raw_response_id = str(batch[-1][0])
        print(
            f"[lineage] raw_ids={len(batch)} updated={updated} total={updated_total}",
            flush=True,
        )
        if updated == 0 and len(batch) < raw_id_limit:
            return updated_total
    return updated_total


def create_temporal_candidates(connection: psycopg.Connection) -> None:
    """Pick the version first observed at or before each multi-version row's event, rejecting ties."""

    connection.execute("DROP TABLE IF EXISTS _fetch_lineage_multi_ids")
    connection.execute(
        """
        CREATE TEMP TABLE _fetch_lineage_multi_ids
            ON COMMIT PRESERVE ROWS AS
        SELECT raw_response_id
        FROM raw_response_versions
        GROUP BY raw_response_id
        HAVING count(*) > 1
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX _fetch_lineage_multi_ids_raw_id_idx
            ON _fetch_lineage_multi_ids(raw_response_id)
        """
    )
    connection.execute("ANALYZE _fetch_lineage_multi_ids")
    connection.execute("DROP TABLE IF EXISTS _fetch_lineage_temporal_candidates")
    connection.execute(
        """
        CREATE TEMP TABLE _fetch_lineage_temporal_candidates
            ON COMMIT PRESERVE ROWS AS
        SELECT fetches.id AS fetch_id,
               chosen.raw_response_version_id
        FROM crawl_run_fetches AS fetches
        JOIN _fetch_lineage_multi_ids AS multi
          ON multi.raw_response_id = fetches.raw_response_id
        CROSS JOIN LATERAL (
            SELECT versions.id AS raw_response_version_id,
                   versions.first_seen_at
            FROM raw_response_versions AS versions
            WHERE versions.raw_response_id = fetches.raw_response_id
              AND versions.first_seen_at <= fetches.fetched_at
            ORDER BY versions.first_seen_at DESC, versions.id DESC
            LIMIT 1
        ) AS chosen
        WHERE fetches.raw_response_id IS NOT NULL
          AND fetches.raw_response_version_id IS NULL
          AND fetches.outcome = 'skipped_existing_backfill'
          AND NOT EXISTS (
              SELECT 1
              FROM raw_response_versions AS tie
              WHERE tie.raw_response_id = fetches.raw_response_id
                AND tie.first_seen_at = chosen.first_seen_at
                AND tie.id <> chosen.raw_response_version_id
          )
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX _fetch_lineage_temporal_candidates_fetch_id_idx
            ON _fetch_lineage_temporal_candidates(fetch_id)
        """
    )
    connection.execute("ANALYZE _fetch_lineage_temporal_candidates")


def temporal_candidate_count(connection: psycopg.Connection) -> int:
    row = connection.execute(
        "SELECT count(*) FROM _fetch_lineage_temporal_candidates"
    ).fetchone()
    return int(row[0] if row else 0)


def repair_temporal(
    connection: psycopg.Connection,
    *,
    batch_rows: int,
    max_fetch_rows: int | None,
) -> int:
    """Apply the precomputed conservative multi-version choices in batches."""

    updated_total = 0
    last_fetch_id = "00000000-0000-0000-0000-000000000000"
    while max_fetch_rows is None or updated_total < max_fetch_rows:
        limit = max(1, min(batch_rows, max_fetch_rows - updated_total) if max_fetch_rows is not None else batch_rows)
        with connection.transaction():
            batch = connection.execute(
                """
                SELECT fetch_id, raw_response_version_id
                FROM _fetch_lineage_temporal_candidates
                WHERE fetch_id > %s::uuid
                ORDER BY fetch_id
                LIMIT %s
                """,
                (last_fetch_id, limit),
            ).fetchall()
            if not batch:
                return updated_total
            connection.execute(
                """
                UPDATE crawl_run_fetches AS fetches
                SET raw_response_version_id = candidates.raw_response_version_id
                FROM _fetch_lineage_temporal_candidates AS candidates
                WHERE candidates.fetch_id = fetches.id
                  AND fetches.raw_response_version_id IS NULL
                  AND fetches.outcome = 'skipped_existing_backfill'
                  AND candidates.fetch_id = ANY(%s::uuid[])
                """,
                ([str(row[0]) for row in batch],),
            )
            updated = len(batch)
        updated_total += updated
        last_fetch_id = str(batch[-1][0])
        print(
            f"[lineage-temporal] rows={updated} total={updated_total}",
            flush=True,
        )
    return updated_total


def main() -> int:
    args = parse_args()
    if args.batch_raw_ids < 1 or args.max_fetch_rows < 0:
        raise SystemExit("--batch-raw-ids must be positive and --max-fetch-rows non-negative")
    max_fetch_rows = None if args.all else args.max_fetch_rows
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    with psycopg.connect(settings.database_url, autocommit=True) as connection:
        connection.execute("SET statement_timeout = '120s'")
        if not args.dry_run:
            ensure_lookup_index(connection)
        create_singleton_map(connection)
        if args.resolve_endpoint:
            create_endpoint_candidates(connection)
        if args.skip_count:
            print("[lineage] repairable_singleton_rows=skipped", flush=True)
        else:
            count = repairable_count(connection)
            print(f"[lineage] repairable_singleton_rows={count}", flush=True)
        if args.dry_run:
            if args.resolve_temporal:
                create_temporal_candidates(connection)
                print(
                    "[lineage-temporal] unique_timestamp_candidates="
                    f"{temporal_candidate_count(connection)}",
                    flush=True,
                )
            if args.resolve_endpoint:
                print(
                    "[lineage-endpoint] candidates="
                    f"{endpoint_candidate_count(connection)}",
                    flush=True,
                )
            return 0
        started = time.monotonic()
        updated = repair(
            connection,
            batch_raw_ids=args.batch_raw_ids,
            max_fetch_rows=max_fetch_rows,
        )
        print(
            f"[lineage] updated={updated} elapsed_seconds={time.monotonic() - started:.1f}",
            flush=True,
        )
        if args.resolve_temporal:
            create_temporal_candidates(connection)
            temporal_count = temporal_candidate_count(connection)
            print(
                f"[lineage-temporal] unique_timestamp_candidates={temporal_count}",
                flush=True,
            )
            temporal_started = time.monotonic()
            temporal_updated = repair_temporal(
                connection,
                batch_rows=max(1, args.batch_raw_ids * 100),
                max_fetch_rows=max_fetch_rows,
            )
            print(
                "[lineage-temporal] updated="
                f"{temporal_updated} elapsed_seconds={time.monotonic() - temporal_started:.1f}",
                flush=True,
            )
        if args.resolve_endpoint:
            endpoint_candidates = endpoint_candidate_count(connection)
            print(
                "[lineage-endpoint] candidates="
                f"{endpoint_candidates}",
                flush=True,
            )
            endpoint_started = time.monotonic()
            endpoint_updated = repair_endpoint(
                connection,
                batch_rows=max(1, args.batch_raw_ids * 100),
                max_fetch_rows=max_fetch_rows,
            )
            print(
                "[lineage-endpoint] updated="
                f"{endpoint_updated} elapsed_seconds={time.monotonic() - endpoint_started:.1f}",
                flush=True,
            )
        remaining_repairable = repairable_count(connection)
        print(
            "[lineage] repairable_singleton_rows_remaining="
            f"{remaining_repairable}",
            flush=True,
        )
        summary = null_version_summary(connection)
        if summary:
            formatted = ",".join(
                f"{outcome}:{total}/with_raw_id={with_raw_id}"
                for outcome, total, with_raw_id in summary
            )
        else:
            formatted = "none"
        print(f"[lineage] null_version_rows_by_outcome={formatted}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
