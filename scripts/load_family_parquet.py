"""Validate an all-family Parquet package; optionally load a NEW Postgres schema.

No database access without --apply and BICAM_EXPORT_TARGET_DSN. Incomplete
candidates additionally require --accept-candidate. Never reads pipeline .env.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

import pyarrow.parquet as pq
import yaml

ROOTS = {"bills", "amendments", "committees", "committee_meetings", "committee_prints",
         "committee_reports", "congresses", "hearings", "members", "nominations", "treaties"}
TYPES = {"text", "integer", "numeric", "numeric_2", "boolean", "date", "timestamptz"}
CONTRACT = Path(__file__).resolve().parents[1] / "contracts" / "bicam_public_schema.yaml"


def expected_tables():
    contract = yaml.safe_load(CONTRACT.read_text())
    core = {r["table"] for family in contract["families"]
            for r in [family, *family.get("children", [])]
            if r["feature_status"] == "core-now"}
    supplemental = {r["table"] for r in contract.get("supplemental_relations", [])
                    if r["feature_status"] == "supplemental"}
    return core, core | supplemental


def validate(bundle):
    manifest = json.loads((bundle / "manifest.json").read_text())
    names = set()
    for table in manifest["tables"]:
        name = table["name"]
        if name in names or not any(name == r or name.startswith(r + "_") for r in ROOTS):
            raise ValueError("invalid/duplicate table name")
        names.add(name)
        path = bundle / table["file"]
        if path.is_symlink() or path.parent.resolve() != bundle.resolve():
            raise ValueError("file outside bundle")
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != table["sha256"]:
                raise ValueError(f"hash mismatch: {name}")
        parquet = pq.ParquetFile(path)
        if parquet.metadata.num_rows != table["row_count"]:
            raise ValueError(f"row count mismatch: {name}")
        if parquet.schema_arrow.names != [c["name"] for c in table["columns"]]:
            raise ValueError(f"columns differ: {name}")
        if any(c["postgres_type"] not in TYPES for c in table["columns"]):
            raise ValueError("unsupported PostgreSQL type")
    core_names, all_names = expected_tables()
    if not ROOTS <= names or (names != core_names and names != all_names):
        raise ValueError("expected all eleven roots plus the declared core/supplemental relations")
    with (bundle / "inclusion.json").open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["inclusion_sha256"]:
            raise ValueError("inclusion ledger hash mismatch")
    if manifest.get("identity_decisions_file"):
        path = bundle / manifest["identity_decisions_file"]
        if path.is_symlink() or path.parent.resolve() != bundle.resolve():
            raise ValueError("identity decision file outside bundle")
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["identity_decisions_sha256"]:
                raise ValueError("identity decision hash mismatch")
    if manifest.get("member_enrichments_file"):
        path = bundle / manifest["member_enrichments_file"]
        if path.is_symlink() or path.parent.resolve() != bundle.resolve():
            raise ValueError("member enrichment file outside bundle")
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["member_enrichments_sha256"]:
                raise ValueError("member enrichment hash mismatch")
    if manifest.get("directory_enrichments_file"):
        path = bundle / manifest["directory_enrichments_file"]
        if path.is_symlink() or path.parent.resolve() != bundle.resolve():
            raise ValueError("directory enrichment file outside bundle")
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["directory_enrichments_sha256"]:
                raise ValueError("directory enrichment hash mismatch")
    if manifest.get("timestamp_enrichments_file"):
        path = bundle / manifest["timestamp_enrichments_file"]
        if path.is_symlink() or path.parent.resolve() != bundle.resolve():
            raise ValueError("timestamp enrichment file outside bundle")
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["timestamp_enrichments_sha256"]:
                raise ValueError("timestamp enrichment hash mismatch")
    return manifest


def load(bundle, manifest, dsn, schema):
    import psycopg
    from psycopg import sql

    with psycopg.connect(dsn) as connection:
        connection.execute("set local lock_timeout = '5s'")
        connection.execute("set local statement_timeout = '15min'")
        connection.execute(sql.SQL("create schema {}").format(sql.Identifier(schema)))
        for table in manifest["tables"]:
            target = sql.Identifier(schema, table["name"])
            definitions = sql.SQL(", ").join(sql.SQL("{} {}").format(
                sql.Identifier(c["name"]), sql.SQL(c["postgres_type"])) for c in table["columns"])
            connection.execute(sql.SQL("create table {} ({})").format(target, definitions))
            columns = [c["name"] for c in table["columns"]]
            with connection.cursor().copy(sql.SQL("copy {} ({}) from stdin").format(
                target, sql.SQL(", ").join(map(sql.Identifier, columns)))) as copy:
                for batch in pq.ParquetFile(bundle / table["file"]).iter_batches(batch_size=4096):
                    for row in batch.to_pylist():
                        copy.write_row([row[c] for c in columns])
            count = connection.execute(sql.SQL("select count(*) from {}").format(target)).fetchone()[0]
            if count != table["row_count"]:
                raise ValueError("loaded row count differs")
            if table.get("primary_key") and table.get("key_semantics") == "unique":
                keys = sql.SQL(",").join(map(sql.Identifier, table["primary_key"]))
                connection.execute(sql.SQL("create unique index on {} ({})").format(target, keys))
            print(json.dumps({"table": table["name"], "loaded_rows": count}), flush=True)
        if not manifest.get("structural_issues"):
            for table in manifest["tables"]:
                for column, target in table.get("foreign_keys", {}).items():
                    target_table, target_column = target.split(".")
                    connection.execute(sql.SQL("alter table {} add foreign key ({}) references {} ({})").format(
                        sql.Identifier(schema, table["name"]), sql.Identifier(column),
                        sql.Identifier(schema, target_table), sql.Identifier(target_column)))
        else:
            print("Candidate has unresolved structural issues; foreign keys not installed.")
    print("Load committed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--schema", default="bicam")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--accept-candidate", action="store_true")
    args = parser.parse_args()
    manifest = validate(args.bundle)
    print(json.dumps({"validated_tables": len(manifest["tables"]), "release_ready": manifest["release_ready"]}))
    if args.apply:
        if not manifest["release_ready"] and not args.accept_candidate:
            parser.error("incomplete candidate: explicit --accept-candidate required")
        dsn = os.environ.get("BICAM_EXPORT_TARGET_DSN")
        if not dsn:
            parser.error("--apply requires BICAM_EXPORT_TARGET_DSN")
        load(args.bundle, manifest, dsn, args.schema)
