"""Offline original-schema bills subset. Requires pyarrow and PyYAML.

Read a frozen CSV snapshot, never PostgreSQL. Output is new-only; manifest.json
is written last, after source digests/counts and Parquet value roundtrips pass.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import csv
import hashlib
import io
import json
from pathlib import Path
import shutil
import zipfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as acsv
import pyarrow.parquet as pq
import yaml


TYPES = {"text": pa.string(), "integer": pa.int32(), "numeric": pa.decimal128(38, 1),
         "numeric_2": pa.decimal128(38, 2),
         "boolean": pa.bool_(), "date": pa.date32(), "timestamptz": pa.timestamp("us", tz="UTC")}
PARENTS = {"bills_reference_codes_sections": ("bill_code_id", "bills_reference_codes"),
           "bills_reference_statutes_pages": ("bill_statute_id", "bills_reference_statutes")}
CHAMBER_COLUMNS = frozenset({"chamber", "origin_chamber", "current_chamber"})


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def digest_rows(digest, rows):
    for row in rows:
        digest.update(json.dumps(row, ensure_ascii=False, default=str,
                                 separators=(",", ":")).encode() + b"\n")


def value(text, kind):
    if text is None or kind == "text":
        return text
    if kind == "integer":
        return int(text)
    if kind in {"numeric", "numeric_2"}:
        number = Decimal(text)
        scale = Decimal("0.01") if kind == "numeric_2" else Decimal("0.1")
        if number != number.quantize(scale):
            raise ValueError(f"numeric precision exceeds {kind} scale; refusing rounding")
        return number.quantize(scale)
    if kind == "boolean":
        if text not in {"t", "f", "true", "false"}:
            raise ValueError(f"invalid boolean {text!r}")
        return text in {"t", "true"}
    if kind == "date":
        return date.fromisoformat(text)
    if kind == "timestamptz":
        result = datetime.fromisoformat(text)
        if result.tzinfo is None:
            raise ValueError("timezone missing")
        return result.astimezone(timezone.utc)
    raise ValueError(kind)


def normalize_chamber(text):
    """Apply the public product's case-only chamber invariant."""

    return text.strip().lower() if text is not None else None


def batches(path, columns):
    return acsv.open_csv(
        path, read_options=acsv.ReadOptions(block_size=32 * 1024 * 1024, use_threads=False),
        parse_options=acsv.ParseOptions(newlines_in_values=True),
        convert_options=acsv.ConvertOptions(column_types={c: pa.string() for c in columns},
                                           null_values=[r"\N"], strings_can_be_null=True,
                                           quoted_strings_can_be_null=False))


def export(snapshot, output, contract_path, archive, since, date_column="introduced_at"):
    if date_column not in {"introduced_at", "updated_at"}:
        raise ValueError("date_column must be introduced_at or updated_at")
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("serialization", {}).get("csv_null") != r"\N":
        raise ValueError("explicit CSV NULL serialization required")
    contract = yaml.safe_load(contract_path.read_text())
    family = next(f for f in contract["families"] if f["name"] == "bills")
    relations = [r for r in [family, *family["children"]] if r["feature_status"] == "core-now"]
    inventory = {r["name"]: r for r in manifest["tables"]}
    with zipfile.ZipFile(archive) as z:
        headers = {}
        for name in z.namelist():
            if name.endswith(".csv") and not name.startswith("__MACOSX/"):
                with z.open(name) as stream:
                    headers[Path(name).name] = next(csv.reader(io.TextIOWrapper(stream, encoding="utf-8-sig")))
    for relation in relations:
        if headers[relation["source_file"]] != list(relation["columns"]):
            raise ValueError(f"original column order differs: {relation['table']}")
    output.mkdir(parents=True, exist_ok=False)
    records, keys = [], {}
    selected_bills = set()
    external_related = set()
    for relation in relations:
        name, columns = relation["table"], relation["columns"]
        source = inventory[name]
        path = snapshot / source["file"]
        if path.parent.resolve() != snapshot.resolve() or sha(path) != source["sha256"]:
            raise ValueError(f"source path/hash mismatch: {name}")
        schema = pa.schema([(c, TYPES[t]) for c, t in columns.items()])
        destination = output / (name + ".parquet")
        read_count = write_count = 0
        expected_digest = hashlib.sha256()
        keys[name] = set()
        with batches(path, columns) as reader, pq.ParquetWriter(destination, schema, compression="zstd") as writer:
            if reader.schema.names != list(columns):
                raise ValueError(f"CSV header differs: {name}")
            for batch in reader:
                read_count += batch.num_rows
                if name == "bills":
                    dates = batch.column(date_column).to_pylist()
                    keep = pa.array([x is not None and value(x, "timestamptz").date() >= since for x in dates])
                else:
                    column, parent = PARENTS.get(name, ("bill_id", "bills"))
                    keep = pc.is_in(batch.column(column), value_set=pa.array(keys[parent], type=pa.string()))
                rows = batch.filter(keep).to_pylist()
                typed = []
                for row in rows:
                    converted = {c: value(row[c], t) for c, t in columns.items()}
                    for column in CHAMBER_COLUMNS.intersection(columns):
                        converted[column] = normalize_chamber(converted[column])
                    typed.append(converted)
                table = pa.Table.from_pylist(typed, schema=schema)
                # Digest actual typed values, including explicit nulls and exact decimals.
                digest_rows(expected_digest, table.to_pylist())
                writer.write_table(table, row_group_size=8192)
                write_count += table.num_rows
                key = {"bills": "bill_id", "bills_actions": "action_id",
                       "bills_reference_codes": "bill_code_id",
                       "bills_reference_statutes": "bill_statute_id"}.get(name)
                if key:
                    for r in typed:
                        if r[key] is None or (name == "bills" and r[key] in keys[name]):
                            raise ValueError(f"null/duplicate root key: {name}")
                        keys[name].add(r[key])
                if name in {"bills_actions_committees", "bills_actions_recorded_votes"}:
                    if any(r["action_id"] not in keys["bills_actions"] for r in typed):
                        raise ValueError("action child lacks exported action")
                if name == "bills_related_bills":
                    external_related.update(r["related_bill_id"] for r in typed
                                            if r["related_bill_id"] and r["related_bill_id"] not in keys["bills"])
            if read_count != source["row_count"]:
                raise ValueError(f"source row count differs: {name}")
        actual_digest = hashlib.sha256()
        parquet = pq.ParquetFile(destination)
        for batch in parquet.iter_batches(batch_size=8192):
            digest_rows(actual_digest, batch.to_pylist())
        if parquet.metadata.num_rows != write_count or actual_digest.digest() != expected_digest.digest():
            raise ValueError(f"Parquet value roundtrip differs: {name}")
        records.append({"name": name, "file": destination.name, "row_count": write_count,
                        "bytes": destination.stat().st_size, "sha256": sha(destination),
                        "typed_rows_sha256": actual_digest.hexdigest(),
                        "source_file": source["file"], "source_sha256": source["sha256"],
                        "source_row_count": read_count,
                        "columns": [{"name": c, "postgres_type": t} for c, t in columns.items()]})
        print(json.dumps({"table": name, "rows": write_count, "source_rows": read_count}), flush=True)
    selected_bills = keys["bills"]
    result = {"schema_version": 1, "format": "parquet", "compression": "zstd",
              "created_at": datetime.now(timezone.utc).isoformat(),
              "source_snapshot": snapshot.resolve().name, "database_snapshot_at": manifest["database_snapshot_at"],
              "source_manifest_sha256": sha(manifest_path), "contract_sha256": sha(contract_path),
              "original_archive_sha256": sha(archive), "pyarrow_version": pa.__version__,
              "filter": {"column": date_column, "inclusive_since": str(since), "timezone": "UTC",
                         "children": "all rows owned by selected bills, including indirect code/statute children"},
              "bill_count": len(selected_bills), "tables": records,
              "external_related_bill_count": len(external_related),
              "normalizations": {"chamber_columns": "trimmed and lowercased in every exported relation"},
              "omissions": [{"table": "bills_lobbied", "reason": "external LobbyView/LDA crosswalk deferred; unavailable, not a verified empty result"}],
              "supplemental_tables_excluded": ["bills_versions", "bills_version_links"],
              "limitations": ["Original column names/order and declared PostgreSQL types, not a claim of historical row or ID-value equivalence.",
                              "Member/committee and related-bill references outside this subset are preserved; external tables are not bundled.",
                              "Source nulls and incomplete text extraction remain; no new scraping or research-release promotion.",
                              "Numeric bill numbers use exact decimal(38,1); exporter rejects finer scale instead of rounding."]}
    shutil.copyfile(Path(__file__).with_name("load_bills_parquet.py"), output / "load_postgres.py")
    # Portable manifest written last: an incomplete export must not be loadable.
    (output / "manifest.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=Path("contracts/bicam_public_schema.yaml"))
    parser.add_argument("--original-archive", type=Path, default=Path("/tmp/bicam-published/bills.zip"))
    parser.add_argument("--since", type=date.fromisoformat, default=date(2025, 1, 1))
    parser.add_argument("--date-column", choices=("introduced_at", "updated_at"), default="introduced_at",
                        help="Bill timestamp used for root selection (default: introduced_at).")
    args = parser.parse_args()
    export(args.snapshot, args.output, args.contract, args.original_archive, args.since, args.date_column)
