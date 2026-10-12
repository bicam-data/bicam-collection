"""Validate a bills Parquet bundle; optionally COPY it into a NEW Postgres schema.

Install pyarrow and psycopg[binary]. No database access without --apply.
Requires BICAM_EXPORT_TARGET_DSN (never reads the pipeline .env/DATABASE_URL).
All DDL/data load occurs in one transaction. Existing schemas are refused.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

import pyarrow.parquet as pq


PG_TYPES = {"text", "integer", "numeric", "boolean", "date", "timestamptz"}


def validate(bundle):
    manifest = json.loads((bundle / "manifest.json").read_text())
    names = set()
    for table in manifest["tables"]:
        name = table["name"]
        if name in names or not (name == "bills" or name.startswith("bills_")):
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
        if any(c["postgres_type"] not in PG_TYPES for c in table["columns"]):
            raise ValueError("unsupported PostgreSQL type")
    if "bills" not in names:
        raise ValueError("missing bills root")
    return manifest


def load(bundle, manifest, dsn, schema):
    import psycopg
    from psycopg import sql

    with psycopg.connect(dsn) as connection:
        connection.execute("set local lock_timeout = '5s'")
        connection.execute("set local statement_timeout = '15min'")
        # No IF NOT EXISTS: never mix with an existing product or partially load it.
        connection.execute(sql.SQL("create schema {}").format(sql.Identifier(schema)))
        for table in manifest["tables"]:
            name = table["name"]
            target = sql.Identifier(schema, name)
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
                raise ValueError(f"loaded row count differs: {name}")
            if name == "bills":
                connection.execute(sql.SQL("alter table {} add primary key (bill_id)").format(target))
            elif "bill_id" in columns:
                connection.execute(sql.SQL("create index on {} (bill_id)").format(target))
            print(json.dumps({"table": name, "loaded_rows": count}), flush=True)
    print("Load committed; external foreign keys intentionally not enforced for this subset.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--schema", default="bicam")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    manifest = validate(args.bundle)
    print(json.dumps({"validated_tables": len(manifest["tables"]), "bills": manifest["bill_count"]}))
    if args.apply:
        dsn = os.environ.get("BICAM_EXPORT_TARGET_DSN")
        if not dsn:
            parser.error("--apply requires BICAM_EXPORT_TARGET_DSN for your destination database")
        load(args.bundle, manifest, dsn, args.schema)
