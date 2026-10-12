from datetime import date, datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import json
import importlib.util
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[1] / "scripts" / f"{name}.py")
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


exporter = module("export_bills_parquet")
batches, value, PARENTS = exporter.batches, exporter.value, exporter.PARENTS
normalize_chamber = exporter.normalize_chamber
export = exporter.export
validate = module("load_bills_parquet").validate


def test_exact_bill_number_and_dates():
    assert value("123.5", "numeric") == Decimal("123.5")
    with pytest.raises(ValueError, match="refusing rounding"):
        value("123.55", "numeric")
    assert value("2025-01-03", "date") == date(2025, 1, 3)
    assert value("2025-01-03 00:00:00+00", "timestamptz") == datetime(2025, 1, 3, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="timezone missing"):
        value("2025-01-03", "timestamptz")
    with pytest.raises(ValueError, match="invalid boolean"):
        value("unknown", "boolean")


def test_export_chamber_values_are_lowercase():
    assert normalize_chamber(" House of Representatives ") == "house of representatives"
    assert normalize_chamber("SENATE") == "senate"
    assert normalize_chamber(None) is None


def test_csv_null_empty_literal_marker_and_multiline(tmp_path):
    path = tmp_path / "input.csv.gz"
    with gzip.open(path, "wt") as f:
        f.write('bill_id,text\nhr1-119,\\N\nhr2-119,""\nhr3-119,"\\N"\nhr4-119,"line1\nline2"\n')
    with batches(path, {"bill_id": "text", "text": "text"}) as reader:
        rows = reader.read_all().to_pylist()
    assert [r["text"] for r in rows] == [None, "", r"\N", "line1\nline2"]


def test_indirect_children_are_not_filtered_by_missing_bill_id():
    assert PARENTS["bills_reference_codes_sections"] == ("bill_code_id", "bills_reference_codes")
    assert PARENTS["bills_reference_statutes_pages"] == ("bill_statute_id", "bills_reference_statutes")


def test_bill_date_column_is_explicit_and_bounded():
    with pytest.raises(ValueError, match="date_column"):
        export(Path("."), Path("."), Path("."), Path("."), date(2025, 1, 24), "last_action_at")


def test_loader_validates_without_database_and_rejects_tampering(tmp_path):
    path = tmp_path / "bills.parquet"
    pq.write_table(pa.table({"bill_id": ["hr1-119"]}), path)
    manifest = {"bill_count": 1, "tables": [{"name": "bills", "file": path.name,
        "row_count": 1, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "columns": [{"name": "bill_id", "postgres_type": "text"}]}]}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    assert validate(tmp_path)["bill_count"] == 1
    path.write_bytes(b"modified")
    with pytest.raises(ValueError, match="hash mismatch"):
        validate(tmp_path)
