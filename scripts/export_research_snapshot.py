"""Export the contract-defined BICAM product as one coherent candidate.

Every core-now relation in ``contracts/bicam_public_schema.yaml`` is exported
from the lowercase ``bicam`` PostgreSQL schema. Files use the contract's
column order and declared deterministic order, and are written inside one
repeatable-read transaction so the manifest and portable provenance sidecar
describe a single database snapshot. Export never changes the mutable
``latest`` pointer; promotion is a separate validated operation.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import importlib.metadata
import importlib.util
import json
import platform
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any, Iterable

import psycopg

from bicam_collection.config import Settings
from bicam_collection.target_scope import TARGET_CONGRESS_CATEGORIES

try:  # Works both as ``python -m scripts...`` and as a direct script.
    from scripts.validate_bicam_product import (
        DEFAULT_CONTRACT,
        _type_matches,
        contract_relations,
        load_contract,
        validate_product_data,
    )
except ModuleNotFoundError:  # pragma: no cover - depends on invocation path
    _validator_spec = importlib.util.spec_from_file_location(
        "_bicam_product_validator", Path(__file__).with_name("validate_bicam_product.py")
    )
    if _validator_spec is None or _validator_spec.loader is None:
        raise ImportError("unable to load validate_bicam_product.py")
    _validator = importlib.util.module_from_spec(_validator_spec)
    _validator_spec.loader.exec_module(_validator)
    DEFAULT_CONTRACT = _validator.DEFAULT_CONTRACT
    _type_matches = _validator._type_matches
    contract_relations = _validator.contract_relations
    load_contract = _validator.load_contract
    validate_product_data = _validator.validate_product_data

try:
    from scripts.export_source_evidence import _query_rows as _query_source_evidence_rows
    from scripts.export_source_evidence import write_bundle as _write_source_evidence_bundle
except ModuleNotFoundError:  # pragma: no cover - direct-script fallback
    _source_evidence_spec = importlib.util.spec_from_file_location(
        "_bicam_source_evidence", Path(__file__).with_name("export_source_evidence.py")
    )
    if _source_evidence_spec is None or _source_evidence_spec.loader is None:
        raise ImportError("unable to load export_source_evidence.py")
    _source_evidence = importlib.util.module_from_spec(_source_evidence_spec)
    _source_evidence_spec.loader.exec_module(_source_evidence)
    _query_source_evidence_rows = _source_evidence._query_rows
    _write_source_evidence_bundle = _source_evidence.write_bundle


# Compatibility alias for callers that imported the old main-table constant.
# Product exports use the full contract plan below.
CORE_EXPORTS: dict[str, str] = {}
RELATION_EXPORTS: dict[str, str] = {}
EXPORTS: dict[str, str] = {}
LINEAGE_COLUMNS = frozenset(
    {
        "source_raw_id",
        "source_raw_response_version_id",
        "raw_id",
        "raw_response_version_id",
        "govinfo_raw_id",
        "govinfo_raw_response_version_id",
        "congress_raw_id",
        "congress_raw_response_version_id",
    }
)
# COPY's explicit NULL marker keeps SQL NULL distinct from a quoted empty
# string in the portable CSV artifacts. Keep this value in the manifest so
# offline consumers do not have to infer encoding semantics.
CSV_NULL_MARKER = r"\N"
try:
    CORE_EXPORTS = {
        relation["table"]: relation["table"]
        for relation in contract_relations(load_contract())
        if relation["table"] in {
            "bills", "amendments", "members", "congresses", "hearings", "committees",
            "committee_meetings", "committee_prints", "committee_reports", "treaties", "nominations",
        }
    }
    EXPORTS = {relation["table"]: relation["table"] for relation in contract_relations(load_contract())}
except (OSError, ValueError, KeyError):  # pragma: no cover - malformed contract is checked at runtime
    pass


def quote_identifier(identifier: str) -> str:
    """Quote one PostgreSQL identifier and reject qualified names."""

    if not identifier or "\x00" in identifier or "." in identifier:
        raise ValueError(f"expected one relation identifier, got {identifier!r}")
    return '"' + identifier.replace('"', '""') + '"'


def lineage_columns(column_names: Iterable[str]) -> list[str]:
    """Retain the old helper for consumers inspecting export metadata."""

    markers = ("raw_id", "response_version_id", "source_endpoint", "evidence")
    return [name for name in column_names if any(marker in name.lower() for marker in markers)]


def _contract_plan(
    contract_path: Path,
    selected: Iterable[str] | None,
    *,
    include_supplemental: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    contract = load_contract(contract_path)
    relations = contract_relations(contract, include_supplemental=include_supplemental)
    by_name = {relation["table"]: relation for relation in relations}
    names = list(by_name) if selected is None else list(selected)
    unknown = [name for name in names if name not in by_name]
    if unknown:
        raise ValueError(f"unknown core contract relation(s): {', '.join(unknown)}")
    if len(names) != len(set(names)):
        raise ValueError("duplicate export relation names are not allowed")
    return contract, [by_name[name] for name in names]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("../reports/exports"),
        help="Directory containing timestamped exports (default: ../reports/exports).",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Contract relation names to export (default: every core-now relation).",
    )
    parser.add_argument(
        "--include-supplemental",
        action="store_true",
        help="Include explicitly declared supplemental relations (for example bills_versions and bills_version_links).",
    )
    parser.add_argument(
        "--include-source-evidence",
        action="store_true",
        help="Embed a bounded content-addressed raw-payload bundle from the same snapshot.",
    )
    parser.add_argument("--source-evidence-limit", type=int, default=100)
    parser.add_argument("--source-evidence-source")
    parser.add_argument("--source-evidence-category")
    parser.add_argument("--source-evidence-node-type")
    parser.add_argument("--source-evidence-endpoint-prefix")
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--schema", default="bicam", help="PostgreSQL schema (default: bicam).")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate contract names and print the export plan without reading table data.",
    )
    return parser.parse_args(argv)


def _relation_metadata(connection: psycopg.Connection, schema: str, relation: str) -> tuple[str, list[dict[str, Any]]]:
    row = connection.execute(
        """
        select c.relkind::text
        from pg_class as c
        join pg_namespace as n on n.oid = c.relnamespace
        where n.nspname = %s and c.relname = %s
        """,
        (schema, relation),
    ).fetchone()
    if row is None or row[0] not in {"r", "p"}:
        raise ValueError(f"{schema}.{relation} is missing or is not a durable table")
    columns = connection.execute(
        """
        select column_name, data_type, udt_name, is_nullable
        from information_schema.columns
        where table_schema = %s and table_name = %s
        order by ordinal_position
        """,
        (schema, relation),
    ).fetchall()
    if not columns:
        raise ValueError(f"{schema}.{relation} has no columns")
    return str(row[0]), [
        {"name": column[0], "data_type": column[1], "udt_name": column[2], "is_nullable": column[3]}
        for column in columns
    ]


def _hash_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def _hash_tree(*roots: Path) -> str:
    """Hash source/configuration files that determine a public build."""

    digest = hashlib.sha256()
    files: list[tuple[str, Path]] = []
    for root in roots:
        if root.is_file():
            files.append((root.name, root))
        elif root.is_dir():
            files.extend(
                (f"{root.name}/{path.relative_to(root)}", path)
                for path in root.rglob("*")
                if path.is_file()
                and "__pycache__" not in path.parts
                and ".pytest_cache" not in path.parts
                and ".git" not in path.parts
                and "target" not in path.parts
                and "logs" not in path.parts
            )
    for relative_name, path in sorted(files, key=lambda item: item[0]):
        relative = relative_name.encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        size, file_digest = _hash_file(path)
        digest.update(size.to_bytes(8, "big"))
        digest.update(file_digest.encode("ascii"))
    return digest.hexdigest()


def _git_value(project_root: Path, *arguments: str) -> str | None:
    """Return one sanitized Git value, or ``None`` outside a Git checkout."""

    try:
        result = subprocess.run(
            ["git", *arguments],
            cwd=project_root,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value or None


def _dependency_revision(path: Path, *, relative_to: Path) -> dict[str, Any]:
    """Describe a local editable dependency without copying its source tree."""

    relative_path = path.relative_to(relative_to).as_posix() if path.is_relative_to(relative_to) else str(path)
    revision = _git_value(path, "rev-parse", "HEAD") if path.is_dir() else None
    status = _git_value(path, "status", "--porcelain") if revision else None
    return {
        "path": relative_path,
        "available": path.is_dir(),
        "git_revision": revision,
        "worktree_dirty": bool(status),
        "source_sha256": _hash_tree(path) if path.is_dir() else None,
    }


def _installed_version(distribution: str) -> str | None:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


def _source_bundle_evidence(project_root: Path) -> dict[str, Any]:
    """Capture the explicit code/configuration identity for a candidate.

    The bundle is metadata, not a claim that raw source payloads are
    distributable.  It makes the advertised reproduction boundary explicit and
    prevents credentials, local environments, caches, and unrelated files from
    being silently included in a release identity.
    """

    allowlist_paths = (
        "src",
        "scripts",
        "dbt_project",
        "specs",
        "contracts",
        "pyproject.toml",
        "uv.lock",
        "Dockerfile",
        "Makefile",
        ".github/workflows/ci.yml",
    )
    roots = tuple(project_root / relative for relative in allowlist_paths if (project_root / relative).exists())
    status = _git_value(project_root, "status", "--porcelain", "--", *allowlist_paths)
    lock_path = project_root / "uv.lock"
    lock_bytes, lock_sha256 = _hash_file(lock_path) if lock_path.is_file() else (None, None)
    return {
        "schema_version": 1,
        "reproduction_level": "metadata-bound; raw payload bundle not included",
        "source_allowlist": list(allowlist_paths),
        "source_tree_sha256": _hash_tree(*roots),
        "git_revision": _git_value(project_root, "rev-parse", "HEAD"),
        "worktree_dirty": bool(status),
        "worktree_status_sha256": hashlib.sha256(status.encode("utf-8")).hexdigest() if status else None,
        "dependency_lock": {
            "path": "uv.lock",
            "bytes": lock_bytes,
            "sha256": lock_sha256,
        },
        "local_dependencies": [
            _dependency_revision(project_root.parent / "rotisserie", relative_to=project_root.parent)
        ],
        "environment": {
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "dbt_core_version": _installed_version("dbt-core"),
        },
    }


def _relation_exists(connection: psycopg.Connection, schema: str, relation: str) -> bool:
    row = connection.execute(
        """
        select exists(
            select 1
            from pg_class as c
            join pg_namespace as n on n.oid = c.relnamespace
            where n.nspname = %s and c.relname = %s
        )
        """,
        (schema, relation),
    ).fetchone()
    return bool(row and row[0])


def _capture_build_evidence(
    connection: psycopg.Connection,
    *,
    schema: str,
    contract: dict[str, Any],
    contract_path: Path,
    project_root: Path,
    table_records: list[dict[str, Any]],
    database_snapshot_at: datetime,
) -> dict[str, Any]:
    """Capture candidate-bound build, frontier, and quality evidence.

    All queries run on the exporter's repeatable-read connection, binding
    these checks to the same database snapshot as the CSV files. This alone
    does NOT establish that all materialized models share a completed build
    generation; independent input/run/row binding is required for that claim.
    Missing quality relations are recorded as incomplete evidence and cannot
    be promoted.
    """

    # Public product relations live in ``bicam``; operational dbt facts use
    # the target schema (normally ``public``) and are deliberately kept out
    # of the consumer-facing archive.
    quality_schema = "public"
    quality_relation_names = (
        "fct_reconciliation_metrics",
        "fct_congress_internal_reciprocity_metrics",
        "fct_root_detail_coverage",
        "fct_collection_coverage",
        "fct_source_gap_queue",
    )
    missing_quality_relations = [
        relation for relation in quality_relation_names if not _relation_exists(connection, quality_schema, relation)
    ]
    quality: dict[str, Any] = {"missing_relations": missing_quality_relations}
    if not missing_quality_relations:
        reconciliation = connection.execute(
            f"""
            select count(*)::bigint,
                   count(*) filter (where quality_status = 'pass')::bigint,
                   count(*) filter (where quality_status <> 'pass')::bigint
            from {quote_identifier(quality_schema)}.{quote_identifier('fct_reconciliation_metrics')}
            """
        ).fetchone()
        reciprocity = connection.execute(
            f"""
            select count(*)::bigint,
                   count(*) filter (where quality_status = 'pass')::bigint,
                   count(*) filter (where quality_status <> 'pass')::bigint,
                   count(*) filter (
                       where relationship_count = 0
                          or contradiction_count > 0
                          or unclassified_count > 0
                   )::bigint
            from {quote_identifier(quality_schema)}.{quote_identifier('fct_congress_internal_reciprocity_metrics')}
            """
        ).fetchone()
        root_coverage = connection.execute(
            f"""
            select coalesce(sum(actionable_missing_detail_count), 0)::bigint
            from {quote_identifier(quality_schema)}.{quote_identifier('fct_root_detail_coverage')}
            where source = 'congress'
              and category = any(%s)
            """,
            (list(TARGET_CONGRESS_CATEGORIES),),
        ).fetchone()
        child_coverage = connection.execute(
            f"""
            select coalesce(sum(child_unsatisfied_fetch_count), 0)::bigint
            from {quote_identifier(quality_schema)}.{quote_identifier('fct_collection_coverage')}
            where source = 'congress'
              and category = any(%s)
            """,
            (list(TARGET_CONGRESS_CATEGORIES),),
        ).fetchone()
        source_gaps = connection.execute(
            f"""
            select count(*) filter (where queue_status in ('open', 'due_for_reprobe'))::bigint,
                   count(*)::bigint
            from {quote_identifier(quality_schema)}.{quote_identifier('fct_source_gap_queue')}
            """
        ).fetchone()
        quality.update(
            {
                "reconciliation": {
                    "row_count": int(reconciliation[0] if reconciliation else 0),
                    "pass_count": int(reconciliation[1] if reconciliation else 0),
                    "nonpass_count": int(reconciliation[2] if reconciliation else 0),
                },
                "reciprocity": {
                    "row_count": int(reciprocity[0] if reciprocity else 0),
                    "pass_count": int(reciprocity[1] if reciprocity else 0),
                    "nonpass_count": int(reciprocity[2] if reciprocity else 0),
                    "release_rejected_count": int(
                        reciprocity[3]
                        if reciprocity is not None and len(reciprocity) > 3
                        else 0
                    ),
                },
                "coverage": {
                    "actionable_missing_detail_count": int(root_coverage[0] if root_coverage else 0),
                    "unsatisfied_child_fetch_count": int(child_coverage[0] if child_coverage else 0),
                },
                "source_gap_queue": {
                    "actionable_count": int(source_gaps[0] if source_gaps else 0),
                    "row_count": int(source_gaps[1] if source_gaps else 0),
                },
            }
        )
    frontiers = [
        {
            "source": str(row[0]),
            "category": str(row[1]),
            "latest_observed_at": row[2].isoformat() if row[2] is not None else None,
            "version_count": int(row[3]),
        }
        for row in connection.execute(
            """
            select source, category, max(last_seen_at), count(*)::bigint
            from raw_response_versions
            group by source, category
            order by source, category
            """
        ).fetchall()
    ]
    contract_size, contract_digest = _hash_file(contract_path)
    pipeline_digest = _hash_tree(
        project_root / "dbt_project",
        project_root / "specs",
        project_root / "contracts",
    )
    source_bundle = _source_bundle_evidence(project_root)
    relation_counts = [
        {"relation": str(record["name"]), "row_count": int(record["row_count"])}
        for record in table_records
    ]
    data_validation = validate_product_data(
        connection,
        contract,
        schema=schema,
        include_supplemental=len(table_records) > len(contract_relations(contract)),
        max_examples=10,
    )
    quality_status = "pass"
    if missing_quality_relations:
        quality_status = "incomplete"
    elif not data_validation.get("valid"):
        quality_status = "fail"
    elif quality.get("reconciliation", {}).get("nonpass_count", 0):
        quality_status = "fail"
    elif quality.get("reciprocity", {}).get("release_rejected_count", 0):
        quality_status = "fail"
    elif quality.get("coverage", {}).get("actionable_missing_detail_count", 0):
        quality_status = "fail"
    elif quality.get("coverage", {}).get("unsatisfied_child_fetch_count", 0):
        quality_status = "fail"
    build_inputs = {
        "contract_version": contract["contract_version"],
        "contract_sha256": contract_digest,
        "pipeline_sha256": pipeline_digest,
        "database_snapshot_at": database_snapshot_at.isoformat(),
        "relation_counts": relation_counts,
        "frontiers": frontiers,
        "quality_status": quality_status,
        "source_bundle_sha256": hashlib.sha256(_render_json(source_bundle).encode("utf-8")).hexdigest(),
    }
    build_id = hashlib.sha256(_render_json(build_inputs).encode("utf-8")).hexdigest()
    source_bundle_sha256 = str(build_inputs["source_bundle_sha256"])
    return {
        "build_id": build_id,
        "status": "complete"
        if not missing_quality_relations and data_validation.get("valid") and quality_status == "pass" and frontiers
        else "incomplete",
        "quality_status": quality_status,
        "contract_version": contract["contract_version"],
        "contract_sha256": contract_digest,
        "contract_bytes": contract_size,
        "pipeline_sha256": pipeline_digest,
        "source_bundle_sha256": source_bundle_sha256,
        "database_snapshot_at": database_snapshot_at.isoformat(),
        "relation_counts": relation_counts,
        "source_frontiers": frontiers,
        "source_bundle": source_bundle,
        "quality": quality,
        "data_validation": data_validation,
    }


def _copy_relation(
    connection: psycopg.Connection,
    *,
    schema: str,
    relation: dict[str, Any],
    destination: Path,
) -> int:
    """Stream one relation in contract column and primary-key order."""

    name = str(relation["table"])
    columns = list(relation["columns"])
    quoted_schema = quote_identifier(schema)
    quoted_relation = quote_identifier(name)
    select_columns = ", ".join(quote_identifier(column) for column in columns)
    # A relation may intentionally have a grouping key rather than a unique
    # public key (for example nominations_nominees, where one position can
    # name multiple people).  Export order is deterministic in either case;
    # it must not be advertised as a uniqueness claim.
    ordering = relation.get("export_order") or relation.get("primary_key") or relation.get("grouping_key") or list(columns)
    order_columns = ", ".join(quote_identifier(column) for column in ordering)
    order_sql = f" order by {order_columns}" if order_columns else ""
    row = connection.execute(f"select count(*) from {quoted_schema}.{quoted_relation}").fetchone()
    row_count = int(row[0] if row else 0)
    with destination.open("wb") as raw_stream:
        # mtime=0 makes checksums reproducible when relation contents are
        # unchanged; generated_at remains in the manifest.
        # Level 6 keeps deterministic bytes (mtime=0) while avoiding the
        # disproportionate CPU cost of gzip's default level 9 on the large
        # extracted HTML/text relations.
        with gzip.GzipFile(fileobj=raw_stream, mode="wb", mtime=0, compresslevel=6) as compressed:
            with connection.cursor() as cursor:
                with cursor.copy(
                    f"copy (select {select_columns} from {quoted_schema}.{quoted_relation}{order_sql}) "
                    f"to stdout with (format csv, header true, null '{CSV_NULL_MARKER}')"
                ) as copy:
                    for chunk in copy:
                        compressed.write(chunk)
    return row_count


def _render_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"


def export_snapshot(
    *,
    database_url: str,
    output_dir: Path,
    table_names: Iterable[str] | None = None,
    generated_at: datetime | None = None,
    dry_run: bool = False,
    include_supplemental: bool = False,
    include_source_evidence: bool = False,
    source_evidence_limit: int = 100,
    source_evidence_filters: dict[str, str | None] | None = None,
    contract_path: Path = DEFAULT_CONTRACT,
    schema: str = "bicam",
) -> dict[str, Any]:
    """Export selected core contract relations or the complete product.

    The returned dictionary includes a private ``_candidate_manifest_path``
    convenience field for schedulers. It is not written into the manifest,
    keeping the artifact independent of its local absolute path.
    """

    if schema != "bicam":
        raise ValueError("product export schema must be lowercase bicam")
    generated_at = (generated_at or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0)
    contract, relations = _contract_plan(
        contract_path,
        table_names,
        include_supplemental=include_supplemental,
    )
    if include_source_evidence and source_evidence_limit <= 0:
        raise ValueError("source_evidence_limit must be positive")
    plan = [{"name": relation["table"], "relation": relation["table"]} for relation in relations]
    base_manifest = {
        "schema_version": 2,
        "contract_version": contract["contract_version"],
        "contract_kind": contract["contract_kind"],
        "product": contract["product"]["name"],
        "schema": schema,
        "generated_at": generated_at.isoformat(),
        "snapshot_directory": generated_at.strftime("%Y%m%dT%H%M%SZ"),
        "scope": {
            "source": "contracts/bicam_public_schema.yaml",
            "feature_status": "core-now+supplemental" if include_supplemental else "core-now",
            "relation_count": len(plan),
            "tables": plan,
        },
        "serialization": {
            "format": "csv.gz",
            "encoding": "UTF-8",
            "newline": "LF",
            "header": True,
            "csv_null": CSV_NULL_MARKER,
        },
    }
    if dry_run:
        base_manifest["dry_run"] = True
        base_manifest["source_evidence_requested"] = include_source_evidence
        return base_manifest

    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_name = generated_at.strftime("%Y%m%dT%H%M%SZ")
    final_dir = output_dir / snapshot_name
    if final_dir.exists():
        raise FileExistsError(f"export snapshot already exists: {final_dir}")
    temporary_dir = Path(tempfile.mkdtemp(prefix=".snapshot-", dir=output_dir))
    try:
        with psycopg.connect(database_url) as connection:
            connection.execute("set transaction isolation level repeatable read")
            connection.execute("set transaction read only")
            snapshot_started_at = connection.execute("select transaction_timestamp()").fetchone()[0]
            source_evidence: dict[str, Any] | None = None
            if include_source_evidence:
                filters = source_evidence_filters or {}
                rows = _query_source_evidence_rows(
                    connection,
                    limit=source_evidence_limit,
                    source=filters.get("source"),
                    category=filters.get("category"),
                    node_type=filters.get("node_type"),
                    endpoint_prefix=filters.get("endpoint_prefix"),
                )
                bundle = _write_source_evidence_bundle(
                    rows,
                    temporary_dir / "source-evidence",
                    selection={
                        "source": filters.get("source"),
                        "category": filters.get("category"),
                        "node_type": filters.get("node_type"),
                        "endpoint_prefix": filters.get("endpoint_prefix"),
                        "limit": source_evidence_limit,
                    },
                    database_snapshot_at=snapshot_started_at,
                )
                bundle_dir = Path(bundle["bundle_dir"]).relative_to(temporary_dir).as_posix()
                source_evidence = {
                    "schema_version": 1,
                    "bundle_path": bundle_dir,
                    "manifest_sha256": bundle["manifest_sha256"],
                    "database_snapshot_at": bundle["database_snapshot_at"],
                    "observation_count": bundle["observation_count"],
                    "unique_payload_count": bundle["unique_payload_count"],
                    "selection": bundle["selection"],
                }
            table_records: list[dict[str, Any]] = []
            for relation in relations:
                name = str(relation["table"])
                relation_kind, actual_columns = _relation_metadata(connection, schema, name)
                expected_columns = list(relation["columns"])
                actual_names = [column["name"] for column in actual_columns]
                if actual_names != expected_columns:
                    raise ValueError(
                        f"{schema}.{name} columns do not match contract: expected {expected_columns}, got {actual_names}"
                    )
                for column in actual_columns:
                    expected_type = relation["columns"][column["name"]]
                    if not _type_matches(expected_type, column["data_type"], column["udt_name"]):
                        raise ValueError(
                            f"{schema}.{name}.{column['name']} type does not match contract: "
                            f"expected {expected_type}, got {column['data_type'] or column['udt_name']}"
                        )
                file_name = f"{name}.csv.gz"
                file_path = temporary_dir / file_name
                row_count = _copy_relation(connection, schema=schema, relation=relation, destination=file_path)
                byte_count, sha256 = _hash_file(file_path)
                table_records.append(
                    {
                        "name": name,
                        "relation": name,
                        "relation_kind": relation_kind,
                        "file": file_name,
                        "format": "csv.gz",
                        "row_count": row_count,
                        "bytes": byte_count,
                        "sha256": sha256,
                        "columns": actual_columns,
                        "contract_columns": relation["columns"],
                        "key_semantics": relation.get("key_semantics", "unique"),
                        "primary_key": list(relation.get("primary_key", [])),
                        "grouping_key": list(relation.get("grouping_key", [])),
                        "export_order": list(relation.get("export_order", [])),
                    }
                )
            build_evidence = _capture_build_evidence(
                connection,
                schema=schema,
                contract=contract,
                contract_path=contract_path,
                project_root=Path(__file__).resolve().parents[1],
                table_records=table_records,
                database_snapshot_at=snapshot_started_at,
            )
            manifest = {
                **base_manifest,
                "database_snapshot_at": snapshot_started_at.isoformat(),
                "tables": table_records,
                "build_evidence": build_evidence,
            }
            if source_evidence is not None:
                manifest["source_evidence"] = source_evidence
        manifest_path = temporary_dir / "manifest.json"
        manifest_path.write_text(_render_json(manifest), encoding="utf-8")
        _, manifest_sha256 = _hash_file(manifest_path)
        evidence = manifest["build_evidence"]
        provenance = {
            "provenance_schema_version": 1,
            "product": manifest["product"],
            "schema": manifest["schema"],
            "contract_kind": manifest["contract_kind"],
            "contract_version": manifest["contract_version"],
            "snapshot_directory": manifest["snapshot_directory"],
            "generated_at": manifest["generated_at"],
            "database_snapshot_at": manifest["database_snapshot_at"],
            "manifest_sha256": manifest_sha256,
            "scope": manifest["scope"],
            "serialization": manifest["serialization"],
            "build_evidence": {
                "build_id": evidence["build_id"],
                "status": evidence["status"],
                "quality_status": evidence["quality_status"],
                "contract_sha256": evidence["contract_sha256"],
                "pipeline_sha256": evidence["pipeline_sha256"],
                "source_bundle_sha256": evidence["source_bundle_sha256"],
                "database_snapshot_at": evidence["database_snapshot_at"],
                "relation_counts": evidence["relation_counts"],
                "source_frontiers": evidence["source_frontiers"],
                "source_bundle": evidence["source_bundle"],
                "quality": evidence["quality"],
                "data_validation": {
                    "valid": evidence["data_validation"]["valid"],
                    "check_count": evidence["data_validation"]["check_count"],
                    "failure_count": evidence["data_validation"]["failure_count"],
                },
            },
        }
        if "source_evidence" in manifest:
            provenance["source_evidence"] = manifest["source_evidence"]
        (temporary_dir / "provenance.json").write_text(_render_json(provenance), encoding="utf-8")
        temporary_dir.rename(final_dir)
        # Do not update ``latest`` here. A separate promotion command first
        # validates this exact candidate's manifest, bytes, headers, and
        # complete relation inventory, then swaps the pointer atomically.
        manifest["_candidate_manifest_path"] = str(final_dir / "manifest.json")
        return manifest
    except Exception:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    manifest = export_snapshot(
        database_url=settings.database_url,
        output_dir=args.output_dir,
        table_names=args.tables,
        dry_run=args.dry_run,
        include_supplemental=args.include_supplemental,
        include_source_evidence=args.include_source_evidence,
        source_evidence_limit=args.source_evidence_limit,
        source_evidence_filters={
            "source": args.source_evidence_source,
            "category": args.source_evidence_category,
            "node_type": args.source_evidence_node_type,
            "endpoint_prefix": args.source_evidence_endpoint_prefix,
        },
        contract_path=args.contract,
        schema=args.schema,
    )
    if args.dry_run:
        print(_render_json(manifest), end="")
    else:
        print(
            f"wrote research snapshot candidate generated_at={manifest['generated_at']} "
            f"tables={len(manifest['tables'])} output={args.output_dir / manifest['snapshot_directory']}"
        )
        print(f"candidate_manifest={manifest['_candidate_manifest_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
