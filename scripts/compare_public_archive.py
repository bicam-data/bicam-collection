"""Compare public table row counts with the published CSV archives (read-only).

Counts CSV rows with the csv module (embedded newlines stay one row), checks the
eleven families and declared archive relations, and counts the core-now
relations in ``bicam``. No row-level comparison.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from pathlib import Path
import sys
from typing import Any, Mapping
import zipfile

import psycopg
from psycopg import sql
import yaml

from bicam_collection.config import Settings

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.validate_public_schema import (  # noqa: E402
    EXPECTED_MAIN_TABLES,
    _tables,
    _validate_table_shape,
)


DEFAULT_CONTRACT = Path(__file__).resolve().parents[1] / "contracts" / "bicam_public_schema.yaml"
DEFAULT_ARCHIVE_DIR = Path("/tmp/bicam-published")
DEFAULT_SCHEMA = "bicam"
CORE_STATUS = "core-now"
CSV_FIELD_SIZE_LIMIT = 1024 * 1024 * 1024


def load_contract(path: Path = DEFAULT_CONTRACT) -> dict[str, Any]:
    """Load and validate the public contract and its exact family structure."""

    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("contract must be a mapping")
    if document.get("contract_kind") != "public_product_schema":
        raise ValueError("contract_kind must be public_product_schema")
    product = document.get("product")
    if not isinstance(product, dict) or product.get("postgres_schema") != DEFAULT_SCHEMA:
        raise ValueError("public product contract must target PostgreSQL schema bicam")
    families = document.get("families")
    if not isinstance(families, list) or len(families) != 11:
        raise ValueError("contract must declare exactly 11 families")
    names = {family.get("name") for family in families if isinstance(family, dict)}
    if names != EXPECTED_MAIN_TABLES:
        raise ValueError(f"main family names differ: {sorted(names)}")
    tables = _tables(document)
    all_names = {str(table["table"]) for table in tables}
    for table in tables:
        _validate_table_shape(table, all_names)
    return document


def contract_tables(document: Mapping[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Return ``(family, relation)`` pairs in contract order, including deferred rows."""

    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    families = document.get("families")
    if not isinstance(families, list):
        raise ValueError("contract families must be a list")
    for family in families:
        if not isinstance(family, dict):
            raise ValueError("contract family must be a mapping")
        children = family.get("children", [])
        if not isinstance(children, list):
            raise ValueError(f"{family.get('name')}: children must be a list")
        for relation in [family, *children]:
            if not isinstance(relation, dict):
                raise ValueError("contract relation must be a mapping")
            pairs.append((family, relation))
    return pairs


def count_csv_rows(stream: io.TextIOBase, expected_header: list[str] | None = None) -> int:
    """Count CSV data records, safely handling quoted embedded newlines."""

    csv.field_size_limit(CSV_FIELD_SIZE_LIMIT)
    reader = csv.reader(stream)
    try:
        header = next(reader)
    except StopIteration:
        if expected_header is not None:
            raise ValueError("CSV is empty; expected a contract header")
        return 0
    if expected_header is not None and header != expected_header:
        raise ValueError(f"CSV header differs from contract: expected {expected_header}, got {header}")
    return sum(1 for _ in reader)


def count_published_rows(
    document: Mapping[str, Any], archive_dir: Path = DEFAULT_ARCHIVE_DIR
) -> dict[str, int]:
    """Count each contract CSV in its declared family archive."""

    counts: dict[str, int] = {}
    for family, relation in contract_tables(document):
        archive_name = family.get("source_archive")
        if not isinstance(archive_name, str) or not archive_name:
            raise ValueError(f"{family.get('table')}: source_archive is required")
        archive_path = archive_dir / archive_name
        if not archive_path.is_file():
            raise ValueError(f"missing published archive: {archive_path}")
        source_file = relation.get("source_file")
        if not isinstance(source_file, str) or not source_file:
            raise ValueError(f"{relation.get('table')}: source_file is required")
        try:
            with zipfile.ZipFile(archive_path) as archive:
                if source_file not in archive.namelist():
                    raise ValueError(f"{relation['table']}: {source_file} is not in {archive_name}")
                with archive.open(source_file) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
                    try:
                        declared_columns = relation.get("columns")
                        if not isinstance(declared_columns, dict):
                            raise ValueError(f"{relation['table']}: columns must be a mapping")
                        counts[str(relation["table"])] = count_csv_rows(text, list(declared_columns))
                    finally:
                        text.detach()
        except zipfile.BadZipFile as error:
            raise ValueError(f"invalid published archive: {archive_path}") from error
    return counts


def _durable_table_names(connection: psycopg.Connection, schema: str) -> set[str]:
    rows = connection.execute(
        """
        select c.relname
        from pg_class as c
        join pg_namespace as n on n.oid = c.relnamespace
        where n.nspname = %s and c.relkind in ('r', 'p')
        """,
        (schema,),
    ).fetchall()
    return {str(row[0]) for row in rows}


def count_current_rows(
    connection: psycopg.Connection,
    table_names: list[str],
    *,
    schema: str = DEFAULT_SCHEMA,
) -> dict[str, int | None]:
    """Count named durable relations with safely composed PostgreSQL identifiers."""

    if not schema or "\x00" in schema:
        raise ValueError(f"invalid PostgreSQL schema identifier: {schema!r}")
    durable = _durable_table_names(connection, schema)
    counts: dict[str, int | None] = {}
    for table in table_names:
        if not table or "\x00" in table:
            raise ValueError(f"invalid PostgreSQL table identifier: {table!r}")
        if table not in durable:
            counts[table] = None
            continue
        query = sql.SQL("select count(*) from {}.{}").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
        row = connection.execute(query).fetchone()
        counts[table] = int(row[0] if row else 0)
    return counts


def _metric(current: int | None, published: int) -> dict[str, Any]:
    return {
        "current_count": current,
        "published_count": published,
        "delta": current - published if current is not None else None,
        "ratio": current / published if current is not None and published else None,
        "empty_current": current == 0,
    }


def compare_counts(
    document: Mapping[str, Any],
    published_counts: Mapping[str, int],
    current_counts: Mapping[str, int | None],
    *,
    schema: str = DEFAULT_SCHEMA,
) -> dict[str, Any]:
    """Build a parity report from independently gathered archive/DB counts."""

    pairs = contract_tables(document)
    expected_names = [str(relation["table"]) for _, relation in pairs]
    if len(expected_names) != len(set(expected_names)):
        raise ValueError("contract table names must be unique")
    missing_current = [
        name
        for _, relation in pairs
        if relation.get("feature_status") == CORE_STATUS
        for name in [str(relation["table"])]
        if name not in current_counts or current_counts[name] is None
    ]
    tables: list[dict[str, Any]] = []
    for family, relation in pairs:
        name = str(relation["table"])
        status = str(relation.get("feature_status", ""))
        published = published_counts.get(name)
        current = current_counts.get(name) if status == CORE_STATUS else None
        record: dict[str, Any] = {
            "family": str(family.get("name", family.get("table", ""))),
            "table": name,
            "feature_status": status,
            "source_archive": family.get("source_archive"),
            "source_file": relation.get("source_file"),
        }
        if published is None:
            record.update(_metric(current, 0))
            record["published_count"] = None
            record["delta"] = None
            record["ratio"] = None
        else:
            record.update(_metric(current, int(published)))
        flags: list[str] = []
        if status != CORE_STATUS:
            flags.append("deferred")
        if current is None and status == CORE_STATUS:
            flags.append("missing_current")
        elif current == 0:
            flags.append("empty_current")
        if published is None:
            flags.append("missing_published")
        record["flags"] = flags
        tables.append(record)
    core_names = [name for name, (_, relation) in zip(expected_names, pairs, strict=True) if relation.get("feature_status") == CORE_STATUS]
    archive_names = set(published_counts)
    current_names = {name for name, count in current_counts.items() if count is not None}
    empty_current = [
        table["table"] for table in tables if table["feature_status"] == CORE_STATUS and table["empty_current"]
    ]
    return {
        "contract_version": document.get("contract_version"),
        "contract_kind": document.get("contract_kind"),
        "schema": schema,
        "family_count": len(document.get("families", [])),
        "contract_table_count": len(expected_names),
        "core_table_count": len(core_names),
        "published_table_count": len(archive_names),
        "current_table_count": len(current_names),
        "empty_current_tables": empty_current,
        "coverage": {
            "families_exact": len(document.get("families", [])) == 11,
            "contract_tables_exact": archive_names == set(expected_names),
            "current_core_tables_exact": current_names == set(core_names),
            "missing_published_tables": [name for name in expected_names if name not in archive_names],
            "extra_published_tables": sorted(archive_names - set(expected_names)),
            "missing_current_tables": missing_current,
            "extra_current_tables": sorted(current_names - set(core_names)),
        },
        "tables": tables,
    }


def compare_product(
    *,
    database_url: str,
    archive_dir: Path = DEFAULT_ARCHIVE_DIR,
    contract_path: Path = DEFAULT_CONTRACT,
    schema: str = DEFAULT_SCHEMA,
    statement_timeout_seconds: int = 120,
) -> dict[str, Any]:
    """Run the bounded read-only archive/database count comparison."""

    if schema != DEFAULT_SCHEMA:
        raise ValueError("public product comparison schema must be lowercase bicam")
    if statement_timeout_seconds <= 0:
        raise ValueError("statement timeout must be positive")
    document = load_contract(contract_path)
    published_counts = count_published_rows(document, archive_dir)
    with psycopg.connect(database_url) as connection:
        connection.execute("set transaction isolation level repeatable read")
        connection.execute("set transaction read only")
        connection.execute(f"set local statement_timeout='{int(statement_timeout_seconds)}s'")
        connection.execute("set local lock_timeout='15s'")
        core_names = [
            str(relation["table"])
            for _, relation in contract_tables(document)
            if relation.get("feature_status") == CORE_STATUS
        ]
        current_counts = count_current_rows(connection, core_names, schema=schema)
    return compare_counts(document, published_counts, current_counts, schema=schema)


def render_markdown(report: Mapping[str, Any]) -> str:
    """Render a compact human-readable Markdown report."""

    coverage = report["coverage"]
    lines = [
        "# BICAM public product archive parity",
        "",
        f"- Schema: `{report['schema']}`",
        f"- Contract families: {report['family_count']} (exact: {'yes' if coverage['families_exact'] else 'no'})",
        f"- Contract tables: {report['contract_table_count']} (exact archive coverage: {'yes' if coverage['contract_tables_exact'] else 'no'})",
        f"- Current core tables: {report['current_table_count']} / {report['core_table_count']}",
        "",
        "| Family | Table | Status | Current | Published | Delta | Ratio | Flags |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for table in report["tables"]:
        ratio = table["ratio"]
        ratio_text = "—" if ratio is None else f"{ratio:.3f}"
        published = "—" if table["published_count"] is None else str(table["published_count"])
        current = "—" if table["current_count"] is None else str(table["current_count"])
        delta = "—" if table["delta"] is None else str(table["delta"])
        flags = ", ".join(table["flags"]) or "—"
        lines.append(
            f"| {table['family']} | `{table['table']}` | {table['feature_status']} | "
            f"{current} | {published} | {delta} | {ratio_text} | {flags} |"
        )
    if coverage["missing_current_tables"]:
        lines.extend(["", "Missing current core tables: " + ", ".join(f"`{name}`" for name in coverage["missing_current_tables"])])
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE_DIR)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--database-url", help="Database URL; defaults to configured project settings.")
    parser.add_argument("--statement-timeout-seconds", type=int, default=120)
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of Markdown.")
    parser.add_argument(
        "--output",
        type=Path,
        help="Also write the rendered report to this durable artifact path.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    database_url = args.database_url
    if database_url is None:
        database_url = Settings.from_env(Path(__file__).resolve().parents[1]).database_url
    try:
        report = compare_product(
            database_url=database_url,
            archive_dir=args.archive_dir,
            contract_path=args.contract,
            schema=args.schema,
            statement_timeout_seconds=args.statement_timeout_seconds,
        )
    except (OSError, ValueError, KeyError, zipfile.BadZipFile, psycopg.Error) as error:
        print(f"public product parity check failed: {error}", file=sys.stderr)
        return 1
    if args.json or args.format == "json":
        rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    else:
        rendered = render_markdown(report)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
