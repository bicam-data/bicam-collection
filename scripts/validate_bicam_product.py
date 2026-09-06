"""Validate a built PostgreSQL ``bicam`` schema against the public contract.

The contract is the source of truth for the public product.  This check does
not inspect dbt model names or the ingestion schema: it compares the durable
relations that a consumer will see, including relation names, column order,
PostgreSQL-compatible types, and any nullability declarations present in the
contract.  Deferred relations (currently ``bills_lobbied``) are intentionally
reported as extra relations if they are materialized.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping
import json
from pathlib import Path
import sys
from typing import Any

import psycopg
from psycopg import sql
import yaml


DEFAULT_CONTRACT = Path(__file__).resolve().parents[1] / "contracts" / "bicam_public_schema.yaml"
RELATIONS_SQL = """
select c.relname, c.relkind::text
from pg_class as c
join pg_namespace as n on n.oid = c.relnamespace
where n.nspname = %s
  and c.relkind in ('r', 'p', 'v', 'm', 'f')
order by c.relname
"""
COLUMNS_SQL = """
select table_name, column_name, ordinal_position, data_type, udt_name, is_nullable
from information_schema.columns
where table_schema = %s
order by table_name, ordinal_position
"""

# The contract vocabulary is deliberately small.  These aliases cover the
# normal information_schema representations and safe PostgreSQL widening for
# integer values.  ``numeric`` is needed for historical bill numbers such as
# 12.5, which cannot be represented by an integer column.
TYPE_ALIASES: dict[str, frozenset[str]] = {
    "text": frozenset({"text", "character varying", "character", "varchar", "bpchar"}),
    "integer": frozenset({"smallint", "integer", "bigint", "int2", "int4", "int8"}),
    "numeric": frozenset({"numeric", "decimal"}),
    "boolean": frozenset({"boolean", "bool"}),
    "date": frozenset({"date"}),
    "timestamptz": frozenset({"timestamp with time zone", "timestamptz"}),
}


def load_contract(path: Path = DEFAULT_CONTRACT) -> dict[str, Any]:
    """Load and minimally validate the public product contract."""

    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("contract must be a mapping")
    if document.get("contract_kind") != "public_product_schema":
        raise ValueError("contract_kind must be public_product_schema")
    product = document.get("product")
    if not isinstance(product, dict) or product.get("postgres_schema") != "bicam":
        raise ValueError("public product contract must target PostgreSQL schema bicam")
    if not isinstance(document.get("contract_version"), int):
        raise ValueError("contract_version must be an integer")
    if not isinstance(document.get("families"), list):
        raise ValueError("contract families must be a list")
    return document


def contract_relations(
    document: Mapping[str, Any],
    *,
    feature_status: str = "core-now",
    include_supplemental: bool = False,
) -> list[dict[str, Any]]:
    """Return declared relations in contract order, including core children.

    Supplemental relations are opt-in so the historical eleven-family/archive
    contract remains stable for downstream consumers.
    """

    result: list[dict[str, Any]] = []
    for family in document.get("families", []):
        if not isinstance(family, dict):
            raise ValueError("contract family must be a mapping")
        for relation in [family, *family.get("children", [])]:
            if not isinstance(relation, dict):
                raise ValueError("contract relation must be a mapping")
            if relation.get("feature_status") == feature_status:
                name = relation.get("table")
                columns = relation.get("columns")
                if not isinstance(name, str) or not name:
                    raise ValueError("contract relation has no table name")
                if not isinstance(columns, dict) or not columns:
                    raise ValueError(f"{name}: columns must be a non-empty mapping")
                result.append(relation)
    if include_supplemental and feature_status == "core-now":
        supplemental = document.get("supplemental_relations", [])
        if not isinstance(supplemental, list):
            raise ValueError("supplemental_relations must be a list")
        for relation in supplemental:
            if not isinstance(relation, dict):
                raise ValueError("supplemental relation must be a mapping")
            if relation.get("feature_status") == "supplemental":
                name = relation.get("table")
                columns = relation.get("columns")
                if not isinstance(name, str) or not name:
                    raise ValueError("supplemental relation has no table name")
                if not isinstance(columns, dict) or not columns:
                    raise ValueError(f"{name}: columns must be a non-empty mapping")
                result.append(relation)
    names = [relation["table"] for relation in result]
    if len(names) != len(set(names)):
        raise ValueError("core contract relation names must be unique")
    return result


def _type_matches(expected: str, actual_data_type: str | None, actual_udt_name: str | None) -> bool:
    allowed = TYPE_ALIASES.get(expected)
    if allowed is None:
        return expected == (actual_data_type or actual_udt_name or "").lower()
    actuals = {
        value.strip().lower()
        for value in (actual_data_type, actual_udt_name)
        if value
    }
    return bool(actuals & allowed)


def _normalise_actual_columns(columns: Iterable[Any]) -> list[dict[str, Any]]:
    """Accept metadata dictionaries or information_schema tuple rows."""

    result: list[dict[str, Any]] = []
    for column in columns:
        if isinstance(column, Mapping):
            result.append(dict(column))
            continue
        values = list(column)
        if len(values) < 2:
            raise ValueError("actual column metadata row is too short")
        result.append(
            {
                "name": values[0],
                "column_name": values[0],
                "ordinal_position": values[1] if len(values) > 1 else None,
                "data_type": values[2] if len(values) > 2 else None,
                "udt_name": values[3] if len(values) > 3 else None,
                "is_nullable": values[4] if len(values) > 4 else None,
            }
        )
    for index, column in enumerate(result, 1):
        column.setdefault("name", column.get("column_name"))
        column.setdefault("ordinal_position", index)
    return result


def compare_schema(
    document: Mapping[str, Any],
    actual_relations: Mapping[str, Iterable[Any]] | Iterable[str],
    *,
    schema: str = "bicam",
    relation_kinds: Mapping[str, str] | None = None,
    include_supplemental: bool = False,
) -> dict[str, Any]:
    """Compare contract relations with supplied database metadata.

    This pure function makes the validator straightforward to unit test
    without a live database.  ``actual_relations`` may be a mapping of name to
    column metadata or an iterable of names (useful when testing missing and
    extra relation reporting).
    """

    core_declared = contract_relations(document)
    expected = contract_relations(document, include_supplemental=include_supplemental)
    all_declared = contract_relations(document, include_supplemental=True)
    supplemental_names = {
        relation["table"]
        for relation in all_declared
        if relation["table"] not in {item["table"] for item in core_declared}
    }
    expected_by_name = {relation["table"]: relation for relation in expected}
    if isinstance(actual_relations, Mapping):
        actual_names = set(actual_relations)
        columns_by_name = {
            name: _normalise_actual_columns(columns)
            for name, columns in actual_relations.items()
        }
    else:
        actual_names = set(actual_relations)
        columns_by_name = {name: [] for name in actual_names}
    expected_names = list(expected_by_name)
    missing_relations = [name for name in expected_names if name not in actual_names]
    # Supplemental relations may exist in the live schema without changing
    # the exact core archive surface.
    extra_relations = sorted(actual_names - set(expected_names) - supplemental_names)
    relation_reports: list[dict[str, Any]] = []
    for name in expected_names:
        expected_relation = expected_by_name[name]
        expected_columns = list(expected_relation["columns"])
        actual_columns = columns_by_name.get(name, [])
        actual_names_in_order = [str(column.get("name", column.get("column_name", ""))) for column in actual_columns]
        expected_set = set(expected_columns)
        actual_set = set(actual_names_in_order)
        missing_columns = [column for column in expected_columns if column not in actual_set]
        extra_columns = [column for column in actual_names_in_order if column not in expected_set]
        type_mismatches: list[dict[str, Any]] = []
        nullability_mismatches: list[dict[str, Any]] = []
        actual_by_name = {str(column.get("name", column.get("column_name", ""))): column for column in actual_columns}
        required_nullability: dict[str, bool | None] = {}
        required_columns = expected_relation.get("required_columns", [])
        if isinstance(required_columns, list):
            for required_column in required_columns:
                actual_required = actual_by_name.get(str(required_column))
                required_nullability[str(required_column)] = (
                    str(actual_required.get("is_nullable", "")).upper() != "NO"
                    if actual_required is not None and actual_required.get("is_nullable") is not None
                    else None
                )
        for column_name, expected_type in expected_relation["columns"].items():
            actual = actual_by_name.get(column_name)
            if actual is None:
                continue
            data_type = actual.get("data_type")
            udt_name = actual.get("udt_name")
            if not _type_matches(str(expected_type), data_type, udt_name):
                type_mismatches.append(
                    {
                        "column": column_name,
                        "expected": expected_type,
                        "actual": data_type or udt_name,
                    }
                )
            expected_nullable = _expected_nullable(expected_relation, column_name)
            if expected_nullable is not None:
                actual_nullable = str(actual.get("is_nullable", "")).upper() != "NO"
                if actual_nullable != expected_nullable:
                    nullability_mismatches.append(
                        {
                            "column": column_name,
                            "expected_nullable": expected_nullable,
                            "actual_nullable": actual_nullable,
                        }
                    )
        report = {
            "relation": name,
            "relation_kind": (relation_kinds or {}).get(name),
            "expected_columns": expected_columns,
            "actual_columns": actual_names_in_order,
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
            "column_order_matches": actual_names_in_order == expected_columns,
            "type_mismatches": type_mismatches,
            "required_nullability": required_nullability,
            "nullability_mismatches": nullability_mismatches,
        }
        report["valid"] = not (
            name in missing_relations
            or (report["relation_kind"] is not None and report["relation_kind"] not in {"r", "p"})
            or missing_columns
            or extra_columns
            or type_mismatches
            or nullability_mismatches
            or not report["column_order_matches"]
        )
        relation_reports.append(report)
    return {
        "valid": not missing_relations and not extra_relations and all(report["valid"] for report in relation_reports),
        "schema": schema,
        "contract_version": document["contract_version"],
        "contract_kind": document["contract_kind"],
        "expected_relation_count": len(expected_names),
        "actual_relation_count": len(actual_names),
        "expected_relations": expected_names,
        "actual_relations": sorted(actual_names),
        "missing_relations": missing_relations,
        "extra_relations": extra_relations,
        "supplemental_relations": sorted(actual_names & supplemental_names),
        "relations": relation_reports,
    }


def _expected_nullable(relation: Mapping[str, Any], column: str) -> bool | None:
    """Read optional nullability declarations without inventing constraints."""

    not_null = relation.get("not_null_columns")
    if isinstance(not_null, list) and column in not_null:
        return False
    nullable = relation.get("nullable_columns")
    if isinstance(nullable, list) and column in nullable:
        return True
    nullable_map = relation.get("nullable")
    if isinstance(nullable_map, Mapping) and column in nullable_map:
        value = nullable_map[column]
        return value if isinstance(value, bool) else None
    return None


def fetch_schema_metadata(connection: psycopg.Connection, schema: str = "bicam") -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
    """Fetch relation and ordered column metadata from PostgreSQL."""

    relation_rows = connection.execute(RELATIONS_SQL, (schema,)).fetchall()
    relation_kinds = {str(row[0]): str(row[1]) for row in relation_rows}
    column_rows = connection.execute(COLUMNS_SQL, (schema,)).fetchall()
    columns: dict[str, list[dict[str, Any]]] = {name: [] for name in relation_kinds}
    for row in column_rows:
        table_name, column_name, ordinal, data_type, udt_name, is_nullable = row[:6]
        columns.setdefault(str(table_name), []).append(
            {
                "name": str(column_name),
                "ordinal_position": ordinal,
                "data_type": str(data_type) if data_type is not None else None,
                "udt_name": str(udt_name) if udt_name is not None else None,
                "is_nullable": str(is_nullable) if is_nullable is not None else None,
            }
        )
    return columns, relation_kinds


def validate_bicam_product(
    connection: psycopg.Connection,
    contract_path: Path = DEFAULT_CONTRACT,
    *,
    schema: str = "bicam",
    include_supplemental: bool = False,
    check_data: bool = False,
    max_examples: int = 10,
) -> dict[str, Any]:
    """Validate the live schema and optionally its row-level contract."""

    document = load_contract(contract_path)
    columns, relation_kinds = fetch_schema_metadata(connection, schema)
    report = compare_schema(
        document,
        columns,
        schema=schema,
        relation_kinds=relation_kinds,
        include_supplemental=include_supplemental,
    )
    if check_data and report["valid"]:
        data_report = validate_product_data(
            connection,
            document,
            schema=schema,
            include_supplemental=include_supplemental,
            max_examples=max_examples,
        )
        report["data_validation"] = data_report
        report["valid"] = bool(report["valid"] and data_report["valid"])
    return report


def _qualified_relation(schema: str, relation: str) -> sql.Composed:
    """Build a safely quoted schema-qualified relation identifier."""

    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(relation))


def _required_value_expression(column: str) -> sql.Composed:
    """Return a NULL/blank-safe required-value predicate.

    Public required fields are not satisfied by a SQL NULL or an empty text
    value.  Casting to text is deliberate here: it preserves zero/false while
    treating only NULL and blank strings as absent, regardless of the source
    column's PostgreSQL scalar type.
    """

    identifier = sql.Identifier(column)
    return sql.SQL("({} IS NULL OR btrim(({})::text) = '')").format(identifier, identifier)


def _composite_foreign_keys(relation: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return additive composite-FK declarations in normalized form.

    Scalar references continue to use ``foreign_keys``.  Relations that need
    a multi-column reference may additionally declare
    ``foreign_key_constraints`` as a list of mappings with
    ``local_columns``, ``target_relation`` and ``target_columns``.  Keeping
    this separate preserves the existing contract format while making
    composite references explicit and auditable.
    """

    declarations = relation.get("foreign_key_constraints", [])
    if declarations is None:
        return []
    if not isinstance(declarations, list):
        raise ValueError("foreign_key_constraints must be a list")
    normalized: list[dict[str, Any]] = []
    for declaration in declarations:
        if not isinstance(declaration, Mapping):
            raise ValueError("foreign_key_constraints entries must be mappings")
        local_columns = declaration.get("local_columns")
        target_relation = declaration.get("target_relation")
        target_columns = declaration.get("target_columns")
        if (
            not isinstance(local_columns, list)
            or not local_columns
            or not all(isinstance(column, (str, int)) and str(column) for column in local_columns)
        ):
            raise ValueError("composite foreign-key local_columns must be a non-empty list")
        if not isinstance(target_relation, str) or not target_relation:
            raise ValueError("composite foreign-key target_relation must be a non-empty string")
        if (
            not isinstance(target_columns, list)
            or not target_columns
            or not all(isinstance(column, (str, int)) and str(column) for column in target_columns)
        ):
            raise ValueError("composite foreign-key target_columns must be a non-empty list")
        if len(local_columns) != len(target_columns):
            raise ValueError("composite foreign-key local and target column counts must match")
        normalized.append(
            {
                "local_columns": [str(column) for column in local_columns],
                "target_relation": target_relation,
                "target_columns": [str(column) for column in target_columns],
            }
        )
    return normalized


def _data_check(
    connection: psycopg.Connection,
    *,
    check_id: str,
    relation: str,
    predicate: sql.Composed,
    select_columns: list[str],
    schema: str,
    max_examples: int,
) -> dict[str, Any]:
    """Run one bounded data check and return machine-readable evidence."""

    columns = [sql.Identifier(column) for column in select_columns]
    query = sql.SQL("select {}, count(*) over () as __violation_count from {} where {} limit %s").format(
        sql.SQL(", ").join(columns),
        _qualified_relation(schema, relation),
        predicate,
    )
    rows = connection.execute(query, (max_examples,)).fetchall()
    count = int(rows[0][-1]) if rows else 0
    examples = [dict(zip(select_columns, row[:-1], strict=True)) for row in rows]
    return {
        "check_id": check_id,
        "relation": relation,
        "violation_count": count,
        "examples": examples,
        "valid": count == 0,
    }


def validate_product_data(
    connection: psycopg.Connection,
    document: Mapping[str, Any],
    *,
    schema: str = "bicam",
    include_supplemental: bool = False,
    max_examples: int = 10,
) -> dict[str, Any]:
    """Validate declared keys, required values, and foreign keys on real rows.

    This is intentionally separate from :func:`compare_schema`: metadata
    validation can prove that a column exists, but only this check can prove
    that a candidate's rows satisfy the declared public grain.  It is opt-in
    for the compatibility CLI so schema-only checks remain inexpensive; the
    candidate-release validator should always enable it.
    """

    if max_examples < 1:
        raise ValueError("max_examples must be positive")
    relations = contract_relations(document, include_supplemental=include_supplemental)
    relation_by_name = {str(relation["table"]): relation for relation in relations}
    checks: list[dict[str, Any]] = []
    for relation in relations:
        name = str(relation["table"])
        columns = relation.get("columns", {})
        if not isinstance(columns, Mapping):
            continue
        required = relation.get("required_columns", [])
        if isinstance(required, list):
            for column in required:
                column_name = str(column)
                if column_name not in columns:
                    checks.append(
                        {
                            "check_id": f"contract.required_column_declared:{name}.{column_name}",
                            "relation": name,
                            "violation_count": 1,
                            "examples": [{"error": "required column is absent from columns map"}],
                            "valid": False,
                        }
                    )
                    continue
                checks.append(
                    _data_check(
                        connection,
                        check_id=f"required_value:{name}.{column_name}",
                        relation=name,
                        predicate=_required_value_expression(column_name),
                        select_columns=[column_name],
                        schema=schema,
                        max_examples=max_examples,
                    )
                )

        key_semantics = str(relation.get("key_semantics", "unique"))
        primary_key = relation.get("primary_key")
        if isinstance(primary_key, list) and primary_key and key_semantics != "grouping":
            key_columns = [str(column) for column in primary_key]
            key_identifiers = [sql.Identifier(column) for column in key_columns]
            null_predicate = sql.SQL(" OR ").join(
                sql.SQL("{} IS NULL").format(identifier) for identifier in key_identifiers
            )
            # A key violation is either a NULL component or a repeated tuple.
            null_query = sql.SQL("select {}, count(*) over () as __violation_count from {} where {} limit %s").format(
                sql.SQL(", ").join(key_identifiers),
                _qualified_relation(schema, name),
                null_predicate,
            )
            null_rows = connection.execute(null_query, (max_examples,)).fetchall()
            null_count = int(null_rows[0][-1]) if null_rows else 0
            checks.append(
                {
                    "check_id": f"key_nonnull:{name}.[{','.join(key_columns)}]",
                    "relation": name,
                    "violation_count": null_count,
                    "examples": [dict(zip(key_columns, row[:-1], strict=True)) for row in null_rows],
                    "valid": null_count == 0,
                }
            )
            duplicate_query = sql.SQL(
                "select {}, count(*) as __violation_count from {} "
                "group by {} having count(*) > 1 "
                "order by __violation_count desc limit %s"
            ).format(
                sql.SQL(", ").join(key_identifiers),
                _qualified_relation(schema, name),
                sql.SQL(", ").join(key_identifiers),
            )
            duplicate_rows = connection.execute(duplicate_query, (max_examples,)).fetchall()
            duplicate_examples = [
                {**dict(zip(key_columns, row[:-1], strict=True)), "duplicate_count": row[-1]}
                for row in duplicate_rows
            ]
            checks.append(
                {
                    "check_id": f"key_unique:{name}.[{','.join(key_columns)}]",
                    "relation": name,
                    "violation_count": len(duplicate_rows),
                    "examples": duplicate_examples,
                    "valid": not duplicate_rows,
                }
            )

        foreign_keys = relation.get("foreign_keys", {})
        if isinstance(foreign_keys, Mapping):
            for local, target in foreign_keys.items():
                local_column = str(local)
                target_text = str(target)
                if "." not in target_text:
                    checks.append(
                        {
                            "check_id": f"contract.foreign_key_declared:{name}.{local_column}",
                            "relation": name,
                            "violation_count": 1,
                            "examples": [{"error": f"invalid target {target_text!r}"}],
                            "valid": False,
                        }
                    )
                    continue
                target_relation, target_column = target_text.split(".", 1)
                local_declared_columns = set(columns)
                if local_column not in local_declared_columns:
                    checks.append(
                        {
                            "check_id": f"contract.foreign_key_local_column:{name}.{local_column}",
                            "relation": name,
                            "violation_count": 1,
                            "examples": [{"error": f"local column {local_column!r} is not declared"}],
                            "valid": False,
                        }
                    )
                    continue
                if target_relation not in relation_by_name:
                    checks.append(
                        {
                            "check_id": f"contract.foreign_key_target:{name}.{local_column}",
                            "relation": name,
                            "violation_count": 1,
                            "examples": [{"error": f"target relation {target_relation!r} is not declared"}],
                            "valid": False,
                        }
                    )
                    continue
                target_columns = relation_by_name[target_relation].get("columns", {})
                if not isinstance(target_columns, Mapping) or target_column not in target_columns:
                    checks.append(
                        {
                            "check_id": f"contract.foreign_key_target_column:{name}.{local_column}",
                            "relation": name,
                            "violation_count": 1,
                            "examples": [
                                {
                                    "error": (
                                        f"target column {target_relation}.{target_column!s} "
                                        "is not declared"
                                    )
                                }
                            ],
                            "valid": False,
                        }
                    )
                    continue
                local_identifier = sql.Identifier(local_column)
                target_identifier = sql.Identifier(target_column)
                query = sql.SQL(
                    "select child.{local}, count(*) over () as __violation_count "
                    "from {schema}.{child} as child "
                    "where child.{local} is not null "
                    "and not exists (select 1 from {schema}.{parent} as parent "
                    "where parent.{target} = child.{local}) limit %s"
                ).format(
                    local=local_identifier,
                    schema=sql.Identifier(schema),
                    child=sql.Identifier(name),
                    parent=sql.Identifier(target_relation),
                    target=target_identifier,
                )
                rows = connection.execute(query, (max_examples,)).fetchall()
                count = int(rows[0][-1]) if rows else 0
                checks.append(
                    {
                        "check_id": f"foreign_key:{name}.{local_column}->{target_text}",
                        "relation": name,
                        "violation_count": count,
                        "examples": [{local_column: row[0]} for row in rows],
                        "valid": count == 0,
                    }
                )
        try:
            composite_foreign_keys = _composite_foreign_keys(relation)
        except ValueError as error:
            checks.append(
                {
                    "check_id": f"contract.composite_foreign_key_declared:{name}",
                    "relation": name,
                    "violation_count": 1,
                    "examples": [{"error": str(error)}],
                    "valid": False,
                }
            )
            composite_foreign_keys = []
        for declaration in composite_foreign_keys:
            local_columns = declaration["local_columns"]
            target_relation = declaration["target_relation"]
            target_columns = declaration["target_columns"]
            label = f"{name}.[{','.join(local_columns)}]->{target_relation}.[{','.join(target_columns)}]"
            local_missing = [column for column in local_columns if column not in columns]
            target_spec = relation_by_name.get(target_relation)
            target_declared_columns = set(target_spec.get("columns", {})) if target_spec else set()
            target_missing = [column for column in target_columns if column not in target_declared_columns]
            if local_missing or target_spec is None or target_missing:
                missing = {
                    "local_columns_missing": local_missing,
                    "target_relation_missing": target_spec is None,
                    "target_columns_missing": target_missing,
                }
                checks.append(
                    {
                        "check_id": f"contract.composite_foreign_key_target:{label}",
                        "relation": name,
                        "violation_count": 1,
                        "examples": [missing],
                        "valid": False,
                    }
                )
                continue
            local_identifiers = [sql.Identifier(column) for column in local_columns]
            target_identifiers = [sql.Identifier(column) for column in target_columns]
            nonnull_predicate = sql.SQL(" AND ").join(
                sql.SQL("child.{} IS NOT NULL").format(identifier) for identifier in local_identifiers
            )
            match_predicate = sql.SQL(" AND ").join(
                sql.SQL("parent.{} = child.{}").format(target_identifier, local_identifier)
                for target_identifier, local_identifier in zip(target_identifiers, local_identifiers, strict=True)
            )
            query = sql.SQL(
                "select {}, count(*) over () as __violation_count "
                "from {schema}.{child} as child "
                "where {nonnull} and not exists (select 1 from {schema}.{parent} as parent where {matches}) "
                "limit %s"
            ).format(
                sql.SQL(", ").join(local_identifiers),
                schema=sql.Identifier(schema),
                child=sql.Identifier(name),
                parent=sql.Identifier(target_relation),
                nonnull=nonnull_predicate,
                matches=match_predicate,
            )
            rows = connection.execute(query, (max_examples,)).fetchall()
            count = int(rows[0][-1]) if rows else 0
            checks.append(
                {
                    "check_id": f"foreign_key:{label}",
                    "relation": name,
                    "violation_count": count,
                    "examples": [
                        dict(zip(local_columns, row[:-1], strict=True)) for row in rows
                    ],
                    "valid": count == 0,
                }
            )
    failures = [check for check in checks if not check["valid"]]
    return {
        "valid": not failures,
        "checks": checks,
        "failures": failures,
        "check_count": len(checks),
        "failure_count": len(failures),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--schema", default="bicam", help="PostgreSQL schema to inspect (default: bicam).")
    parser.add_argument("--database-url", help="Database URL; defaults to configured project settings.")
    parser.add_argument(
        "--include-supplemental",
        action="store_true",
        help="Require and validate supplemental release relations as well as the 11-family core.",
    )
    parser.add_argument(
        "--check-data",
        action="store_true",
        help="Validate row-level keys, required values, and foreign keys in addition to schema metadata.",
    )
    parser.add_argument(
        "--max-examples",
        type=int,
        default=10,
        help="Maximum violating examples retained per data check (default: 10).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    database_url = args.database_url
    if database_url is None:
        from bicam_collection.config import Settings

        database_url = Settings.from_env(Path(__file__).resolve().parents[1]).database_url
    try:
        with psycopg.connect(database_url) as connection:
            connection.execute("set transaction read only")
            report = validate_bicam_product(
                connection,
                args.contract,
                schema=args.schema,
                include_supplemental=args.include_supplemental,
                check_data=args.check_data,
                max_examples=args.max_examples,
            )
    except (OSError, ValueError, KeyError, psycopg.Error) as error:
        print(json.dumps({"valid": False, "error": str(error)}, indent=2, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
