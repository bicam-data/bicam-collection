"""Validate the public BICAM table contract against the published CSV archives."""

from __future__ import annotations

import argparse
import csv
import io
import sys
import zipfile
from pathlib import Path
from typing import Any

import yaml


EXPECTED_MAIN_TABLES = {
    "bills",
    "amendments",
    "committees",
    "committee_meetings",
    "committee_prints",
    "committee_reports",
    "congresses",
    "hearings",
    "members",
    "nominations",
    "treaties",
}
VALID_FEATURE_STATUSES = {"core-now", "deferred-external-crosswalk"}
PG_TYPES = {
    "text",
    "integer",
    "numeric",
    "numeric_2",
    "boolean",
    "date",
    "timestamptz",
}


def _fail(message: str) -> None:
    raise ValueError(message)


def _tables(document: dict[str, Any]) -> list[dict[str, Any]]:
    families = document.get("families")
    if not isinstance(families, list) or len(families) != 11:
        _fail("contract must declare exactly 11 families")
    names = {f.get("name") for f in families if isinstance(f, dict)}
    if names != EXPECTED_MAIN_TABLES:
        _fail(f"main family names differ: {sorted(names)}")
    tables: list[dict[str, Any]] = []
    for family in families:
        if not isinstance(family, dict):
            _fail("family must be a mapping")
        if family.get("table") != family.get("name"):
            _fail(f"main table must use family name: {family.get('name')}")
        tables.append(family)
        children = family.get("children", [])
        if not isinstance(children, list):
            _fail(f"children must be a list: {family['name']}")
        for child in children:
            if not isinstance(child, dict):
                _fail(f"child must be a mapping: {family['name']}")
            table = child.get("table")
            if not isinstance(table, str) or not table.startswith(f"{family['table']}_"):
                _fail(f"child table is not plural-prefixed: {table}")
            tables.append(child)
    names = [t.get("table") for t in tables]
    if any(not isinstance(name, str) for name in names) or len(names) != len(set(names)):
        _fail("table names must be non-empty and unique")
    return tables


def _validate_table_shape(table: dict[str, Any], all_tables: set[str]) -> None:
    name = table["table"]
    columns = table.get("columns")
    if not isinstance(columns, dict) or not columns:
        _fail(f"{name}: columns must be a non-empty mapping")
    if any(not isinstance(column, str) or value not in PG_TYPES for column, value in columns.items()):
        _fail(f"{name}: columns must use the declared PostgreSQL type vocabulary")
    required = table.get("required_columns")
    if not isinstance(required, list) or not required or not set(required).issubset(columns):
        _fail(f"{name}: required_columns must name existing columns")
    primary_key = table.get("primary_key")
    key_semantics = table.get("key_semantics", "unique")
    grouping_key = table.get("grouping_key")
    if primary_key is not None:
        if key_semantics != "unique":
            _fail(f"{name}: primary_key requires key_semantics=unique")
        if not isinstance(primary_key, list) or not primary_key or not set(primary_key).issubset(columns):
            _fail(f"{name}: primary_key must name existing columns")
        if not set(primary_key).issubset(required):
            _fail(f"{name}: primary key columns must be required")
    elif key_semantics == "grouping":
        if not isinstance(grouping_key, list) or not grouping_key or not set(grouping_key).issubset(columns):
            _fail(f"{name}: grouping_key must name existing columns when key_semantics=grouping")
        if not set(grouping_key).issubset(required):
            _fail(f"{name}: grouping key columns must be required")
    else:
        _fail(f"{name}: declare a primary_key or an explicit grouping key")
    export_order = table.get("export_order")
    if export_order is not None:
        if not isinstance(export_order, list) or not export_order or not set(export_order).issubset(columns):
            _fail(f"{name}: export_order must name existing columns")
    if table.get("feature_status") not in VALID_FEATURE_STATUSES:
        _fail(f"{name}: invalid feature status")
    foreign_keys = table.get("foreign_keys", {})
    if not isinstance(foreign_keys, dict):
        _fail(f"{name}: foreign_keys must be a mapping")
    for column, target in foreign_keys.items():
        if column not in columns or not isinstance(target, str) or "." not in target:
            _fail(f"{name}: malformed foreign key {column} -> {target}")
        target_table, target_column = target.rsplit(".", 1)
        if target_table not in all_tables or not target_column:
            _fail(f"{name}: foreign key target is not a contract table: {target}")


def validate_public_schema(contract_path: Path, archive_dir: Path) -> tuple[int, int]:
    document = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        _fail("contract must be a mapping")
    if document.get("contract_kind") != "public_product_schema":
        _fail("contract_kind must be public_product_schema")
    if document.get("product", {}).get("postgres_schema") != "bicam":
        _fail("public schema must be named bicam")
    families = document["families"]
    tables = _tables(document)
    all_table_names = {table["table"] for table in tables}
    for table in tables:
        _validate_table_shape(table, all_table_names)

    expected_csvs = 0
    for family in families:
        archive_name = family.get("source_archive")
        archive_path = archive_dir / archive_name
        if not archive_path.is_file():
            _fail(f"missing published archive: {archive_path}")
        family_tables = [family, *family.get("children", [])]
        expected_csvs += len(family_tables)
        with zipfile.ZipFile(archive_path) as archive:
            members = set(archive.namelist())
            for table in family_tables:
                source_file = table.get("source_file")
                if not source_file:
                    _fail(f"{table['table']}: source_file is required")
                if source_file not in members:
                    _fail(f"{table['table']}: {source_file} is not in {archive_name}")
                with archive.open(source_file) as raw:
                    header = next(csv.reader(io.TextIOWrapper(raw, encoding="utf-8", newline="")))
                declared = list(table["columns"])
                if header != declared:
                    _fail(
                        f"{table['table']}: columns differ from {source_file}; "
                        f"contract={declared}, archive={header}"
                    )
    return len(families), len(tables)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "contracts" / "bicam_public_schema.yaml",
    )
    parser.add_argument("--archive-dir", type=Path, default=Path("/tmp/bicam-published"))
    args = parser.parse_args()
    try:
        families, tables = validate_public_schema(args.contract, args.archive_dir)
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        print(f"public schema validation failed: {error}", file=sys.stderr)
        return 1
    print(f"validated {families} families and {tables} bicam tables against published CSV archives")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
