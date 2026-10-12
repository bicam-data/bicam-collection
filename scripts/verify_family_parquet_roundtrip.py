"""Read-only, all-row Parquet/PostgreSQL comparison for an original-format bundle.

Requires an explicit destination DSN; never reads pipeline settings. Compares
typed row multisets, including duplicate multiplicity, plus declared FK targets.
This verifies transfer fidelity, not source completeness or release readiness.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

import psycopg
from psycopg import sql
import pyarrow.parquet as pq

from export_bills_parquet import sha
from load_family_parquet import validate


def row_digest(row):
    return hashlib.sha256(json.dumps(row, ensure_ascii=False, default=str,
                                    separators=(",", ":")).encode()).digest()


def verify(bundle, manifest, connection, schema):
    results = []
    for table in manifest["tables"]:
        columns = [c["name"] for c in table["columns"]]
        expected = Counter()
        for batch in pq.ParquetFile(bundle / table["file"]).iter_batches(batch_size=4096):
            expected.update(row_digest(row) for row in batch.to_pylist())
        actual = Counter()
        with connection.cursor(name="typed_export_verification") as cursor:
            cursor.execute(sql.SQL("select {} from {}").format(
                sql.SQL(",").join(map(sql.Identifier, columns)), sql.Identifier(schema, table["name"])))
            while rows := cursor.fetchmany(4096):
                actual.update(row_digest(dict(zip(columns, row, strict=True))) for row in rows)
        if actual != expected:
            raise ValueError(f"typed row multiset differs: {table['name']}")
        results.append({"table": table["name"], "rows": sum(actual.values()), "typed_multiset_equal": True})
        print(json.dumps(results[-1]), flush=True)
    foreign_keys = connection.execute("""
        select child.relname, cc.attname, pn.nspname, parent.relname, pc.attname,
               c.convalidated, cardinality(c.conkey), cardinality(c.confkey)
        from pg_constraint c
        join pg_class child on child.oid = c.conrelid
        join pg_namespace cn on cn.oid = child.relnamespace
        join pg_class parent on parent.oid = c.confrelid
        join pg_namespace pn on pn.oid = parent.relnamespace
        join pg_attribute cc on cc.attrelid = child.oid and cc.attnum = c.conkey[1]
        join pg_attribute pc on pc.attrelid = parent.oid and pc.attnum = c.confkey[1]
        where c.contype = 'f' and cn.nspname = %s
    """, (schema,)).fetchall()
    if any(not r[5] or r[6:] != (1, 1) for r in foreign_keys):
        raise ValueError("unvalidated or unexpected composite foreign key")
    expected_keys = Counter((table["name"], column, schema, *target.split("."))
                            for table in manifest["tables"] for column, target in table.get("foreign_keys", {}).items())
    if Counter(row[:5] for row in foreign_keys) != expected_keys:
        raise ValueError("installed foreign key definitions differ from manifest")
    return {"schema_version": 1, "candidate_manifest_sha256": sha(bundle / "manifest.json"),
            "verified_at": datetime.now(timezone.utc).isoformat(), "tables": results,
            "tables_verified": len(results), "rows_verified": sum(r["rows"] for r in results),
            "foreign_key_definitions_verified": len(foreign_keys), "database_schema": schema,
            "read_only": True, "release_accepted": False,
            "limitation": "Transfer fidelity and declared relational constraints only; not source or feature completeness."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--schema", default="bicam")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve prior validation")
    dsn = os.environ.get("BICAM_EXPORT_TARGET_DSN")
    if not dsn:
        parser.error("BICAM_EXPORT_TARGET_DSN is required")
    manifest = validate(args.bundle)
    with psycopg.connect(dsn, options=("-c default_transaction_read_only=on -c statement_timeout=60000 "
                                      "-c lock_timeout=1000 -c timezone=UTC")) as connection:
        report = verify(args.bundle, manifest, connection, args.schema)
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
