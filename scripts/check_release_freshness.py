"""Fail when the latest snapshot is stale, its files drifted, or the materialized
quality and reciprocity metrics regressed (read-only).
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

import psycopg

from bicam_collection.config import Settings


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("../reports/exports/latest/manifest.json"),
        help="Snapshot manifest to verify (default: ../reports/exports/latest/manifest.json).",
    )
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=24.0,
        help="Maximum permitted manifest age (default: 24 hours).",
    )
    parser.add_argument(
        "--max-stale-run-hours",
        type=float,
        default=3.0,
        help="Maximum age of a running crawler before it is an error (default: 3 hours).",
    )
    parser.add_argument(
        "--no-file-verification",
        action="store_true",
        help="Skip SHA-256 and CSV-aware row-count checks (database checks still run).",
    )
    return parser.parse_args(argv)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include a timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _verify_file(path: Path, expected_hash: str, expected_rows: int) -> tuple[int, str]:
    if not path.is_file():
        raise ValueError(f"missing export file: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as binary:
        for chunk in iter(lambda: binary.read(1024 * 1024), b""):
            digest.update(chunk)
    actual_hash = digest.hexdigest()
    if actual_hash != expected_hash:
        raise ValueError(f"SHA-256 mismatch for {path.name}")
    # Text fields exceed csv's 128 KiB default; the manifest already verified the bytes.
    csv.field_size_limit(sys.maxsize)
    with gzip.open(path, "rt", newline="") as stream:
        actual_rows = sum(1 for _ in csv.reader(stream)) - 1
    if actual_rows != expected_rows:
        raise ValueError(
            f"row-count mismatch for {path.name}: expected {expected_rows}, got {actual_rows}"
        )
    return actual_rows, actual_hash


def _verify_manifest(manifest_path: Path, *, now: datetime, max_age_hours: float, verify_files: bool) -> dict[str, Any]:
    if not manifest_path.is_file():
        raise ValueError(f"manifest does not exist: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"manifest is not valid JSON: {manifest_path}") from exc
    generated_at = _parse_timestamp(str(manifest.get("generated_at", "")))
    age_hours = (now - generated_at).total_seconds() / 3600
    if age_hours < -0.01:
        raise ValueError("manifest generated_at is in the future")
    if age_hours > max_age_hours:
        raise ValueError(f"manifest is {age_hours:.2f} hours old (limit {max_age_hours:.2f})")
    tables = manifest.get("tables")
    if not isinstance(tables, list) or not tables:
        raise ValueError("manifest has no exported tables")
    verified_files = 0
    if verify_files:
        for table in tables:
            if not isinstance(table, dict):
                raise ValueError("manifest table entry is not an object")
            file_name = table.get("file")
            if not isinstance(file_name, str) or Path(file_name).name != file_name:
                raise ValueError(f"unsafe or missing export filename: {file_name!r}")
            expected_hash = table.get("sha256")
            expected_rows = table.get("row_count")
            if not isinstance(expected_hash, str) or not isinstance(expected_rows, int):
                raise ValueError(f"manifest lacks hash/row count for {file_name}")
            _verify_file(manifest_path.parent / file_name, expected_hash, expected_rows)
            verified_files += 1
    return {
        "generated_at": generated_at.isoformat(),
        "age_hours": round(age_hours, 3),
        "table_count": len(tables),
        "verified_files": verified_files,
    }


def _database_checks(connection: psycopg.Connection, *, max_stale_run_hours: float) -> dict[str, Any]:
    root = connection.execute(
        """
        select coalesce(sum(actionable_missing_detail_count), 0),
               coalesce(sum(confirmed_source_error_detail_count), 0)
        from fct_root_detail_coverage
        where source = 'congress'
          and category = any(%s)
        """,
        (
            [
                "bills",
                "amendments",
                "members",
                "congresses",
                "hearings",
                "committees",
                "committee-meetings",
                "committee-prints",
                "committee-reports",
                "treaties",
                "nominations",
            ],
        ),
    ).fetchone()
    child = connection.execute(
        """
        select coalesce(sum(child_unsatisfied_fetch_count), 0)
        from fct_collection_coverage
        where source = 'congress'
          and category = any(%s)
        """,
        (
            [
                "bills",
                "amendments",
                "members",
                "congresses",
                "hearings",
                "committees",
                "committee-meetings",
                "committee-prints",
                "committee-reports",
                "treaties",
                "nominations",
            ],
        ),
    ).fetchone()
    reconciliation = connection.execute(
        """
        select count(*) filter (where quality_status <> 'pass'),
               count(*) filter (where open_exception_count > 0),
               count(*) filter (where unexcused_unmatched_count > 0)
        from fct_reconciliation_metrics
        """
    ).fetchone()
    reciprocity = connection.execute(
        """
        select count(*)::bigint,
               count(*) filter (where release_acceptance_status is null
                   or release_acceptance_status not in
                   ('accepted', 'accepted_with_source_boundaries'))::bigint,
               coalesce(sum(contradiction_count), 0)::bigint,
               coalesce(sum(unclassified_count), 0)::bigint
        from fct_congress_internal_reciprocity_metrics
        """
    ).fetchone()
    govinfo_identity = connection.execute(
        """
        select count(*) filter (
                   where metric_name = any(%s)
                     and metric_value > 0
               )::bigint
        from fct_govinfo_data_quality_metrics
        """,
        (
            [
                "committee_id_unmapped_identifier",
                "committee_id_non_identifier_text",
                "committee_id_map_unmapped_identifier",
                "committee_id_map_non_identifier_text",
                "member_provided_invalid",
                "member_multi_legislator_review",
                "submittedby_multi_legislator_review",
                "nested_invalid_bioguide",
                "nested_non_identifier_text",
                "member_component_pending_review",
                "member_component_ambiguous",
                "member_component_no_candidate",
                "member_reference_unresolved_missing_source_identifier",
                "member_reference_pending_review",
                "member_reference_ambiguous",
                "member_reference_multi_legislator_review",
                "member_reference_component_pending_review",
                "member_reference_component_ambiguous",
                "member_reference_component_no_candidate",
            ],
        ),
    ).fetchone()
    stale_runs = connection.execute(
        """
        select count(*)
        from crawl_runs
        where status = 'running'
          and started_at < now() - (%s * interval '1 hour')
        """,
        (max_stale_run_hours,),
    ).fetchone()
    return {
        "actionable_missing_details": int(root[0] if root else 0),
        "classified_source_error_details": int(root[1] if root else 0),
        "unsatisfied_child_fetches": int(child[0] if child else 0),
        "reconciliation_nonpass_rows": int(reconciliation[0] if reconciliation else 0),
        "reconciliation_open_exception_rows": int(reconciliation[1] if reconciliation else 0),
        "reconciliation_unexcused_unmatched_rows": int(reconciliation[2] if reconciliation else 0),
        "reciprocity_metric_family_count": int(reciprocity[0] if reciprocity else 0),
        "reciprocity_release_rejected_count": int(reciprocity[1] if reciprocity else 0),
        "reciprocity_contradiction_count": int(reciprocity[2] if reciprocity else 0),
        "reciprocity_unclassified_count": int(reciprocity[3] if reciprocity else 0),
        "govinfo_identity_quality_issue_count": int(govinfo_identity[0] if govinfo_identity else 0),
        "stale_running_crawl_runs": int(stale_runs[0] if stale_runs else 0),
    }


def check_release(
    *,
    database_url: str,
    manifest_path: Path,
    max_age_hours: float = 24.0,
    max_stale_run_hours: float = 3.0,
    verify_files: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    if max_age_hours < 0 or max_stale_run_hours < 0:
        raise ValueError("age limits must be non-negative")
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    manifest_check = _verify_manifest(
        manifest_path,
        now=now,
        max_age_hours=max_age_hours,
        verify_files=verify_files,
    )
    with psycopg.connect(database_url) as connection:
        connection.execute("set transaction read only")
        connection.execute("set statement_timeout='120s'")
        database_check = _database_checks(connection, max_stale_run_hours=max_stale_run_hours)
    issues = [
        name
        for name, value in database_check.items()
        if name not in {"classified_source_error_details", "reciprocity_metric_family_count"}
        and value
    ]
    if database_check.get("reciprocity_metric_family_count") != 11:
        issues.append("reciprocity_metric_family_count")
    return {
        "status": "pass" if not issues else "fail",
        "checked_at": now.isoformat(),
        "manifest": manifest_check,
        "database": database_check,
        "issues": issues,
    }


def main() -> int:
    args = parse_args()
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    try:
        result = check_release(
            database_url=settings.database_url,
            manifest_path=args.manifest,
            max_age_hours=args.max_age_hours,
            max_stale_run_hours=args.max_stale_run_hours,
            verify_files=not args.no_file_verification,
        )
    except (OSError, ValueError, psycopg.Error) as exc:
        print(f"RELEASE FRESHNESS FAILED: {exc}")
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    print(f"RELEASE FRESHNESS {result['status'].upper()}")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
