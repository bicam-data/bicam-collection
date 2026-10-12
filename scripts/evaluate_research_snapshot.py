"""Produce bounded, offline quality metrics for an immutable BICAM candidate.

The evaluator never connects to PostgreSQL.  It streams the candidate's
compressed CSVs and reports row counts, NULL/blank missingness, Congress and
chamber coverage, extracted-text availability, and bill-version link outcomes.
It deliberately reports descriptive metrics rather than inventing linkage
labels or treating source eligibility as population completeness.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Iterable, Iterator


TEXT_COLUMNS = frozenset({"raw_text", "text"})
LINK_COLUMNS = frozenset({"formatted_text", "pdf", "html", "xml", "png"})
DIMENSION_COLUMNS = ("congress", "chamber", "origin_chamber", "source")


def _csv_rows(stream: Iterable[str], null_marker: str) -> Iterator[list[str | None]]:
    """Read PostgreSQL CSV while retaining its quoted-versus-unquoted NULL semantics.

    csv.reader handles multiline/escaped fields; the physical record lets us
    recover quoting without mistaking a quoted literal marker for SQL NULL.
    Memory is bounded by one record, including large extracted documents.
    """
    physical_lines: list[str] = []

    def lines() -> Iterator[str]:
        for line in stream:
            physical_lines.append(line)
            yield line

    reader = csv.reader(lines(), strict=True)
    for values in reader:
        raw = "".join(physical_lines)
        physical_lines.clear()
        position = 0
        decoded: list[str | None] = []
        for value in values:
            quoted = raw[position:position + 1] == '"'
            decoded.append(None if not quoted and value == null_marker else value)
            position += len(value) + (value.count('"') + 2 if quoted else 0)
            if raw[position:position + 1] not in {",", "\r", "\n", ""}:
                raise ValueError("invalid PostgreSQL CSV field boundary")
            position += 1
        yield decoded


def _hash_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def _missing_kind(value: str | None) -> str | None:
    if value is None:
        return "null"
    if not value.strip():
        return "blank"
    return None


def _counter_json(counter: Counter[str]) -> dict[str, int]:
    return {str(key): int(counter[key]) for key in sorted(counter)}


def _table_report(
    manifest_entry: dict[str, Any],
    candidate_dir: Path,
    *,
    null_marker: str,
    max_rows: int | None,
) -> dict[str, Any]:
    name = str(manifest_entry["name"])
    file_name = manifest_entry.get("file")
    if file_name != f"{name}.csv.gz" or Path(str(file_name)).name != file_name:
        raise ValueError(f"{name}: unexpected artifact filename {file_name!r}")
    path = candidate_dir / str(file_name)
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{name}: artifact is missing or symlinked")
    size, digest = _hash_file(path)
    if size != manifest_entry.get("bytes") or digest != manifest_entry.get("sha256"):
        raise ValueError(f"{name}: artifact bytes differ from manifest")

    csv.field_size_limit(sys.maxsize)
    null_counts: Counter[str] = Counter()
    blank_counts: Counter[str] = Counter()
    dimensions: dict[str, Counter[str]] = {column: Counter() for column in DIMENSION_COLUMNS}
    text_columns: list[str] = []
    link_columns: list[str] = []
    rows_with_text = 0
    rows_with_links = 0
    rows_with_text_and_links = 0
    actual_rows = 0
    complete_scan = True
    with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
        reader = _csv_rows(stream, null_marker)
        try:
            header = next(reader)
        except StopIteration as error:
            raise ValueError(f"{name}: empty CSV artifact") from error
        expected_header = [str(column["name"]) for column in manifest_entry.get("columns", [])]
        if not expected_header or header != expected_header or len(set(header)) != len(header):
            raise ValueError(f"{name}: CSV header differs from manifest")
        text_columns = [column for column in header if column in TEXT_COLUMNS]
        link_columns = [column for column in header if column in LINK_COLUMNS]
        indexes = {column: index for index, column in enumerate(header)}
        for row in reader:
            if max_rows is not None and actual_rows >= max_rows:
                complete_scan = False
                break
            actual_rows += 1
            if len(row) != len(header):
                raise ValueError(f"{name}: row {actual_rows} has {len(row)} fields; expected {len(header)}")
            for column, index in indexes.items():
                kind = _missing_kind(row[index])
                if kind == "null":
                    null_counts[column] += 1
                elif kind == "blank":
                    blank_counts[column] += 1
            for column in DIMENSION_COLUMNS:
                index = indexes.get(column)
                if index is None:
                    continue
                value = row[index]
                if _missing_kind(value) is None:
                    dimensions[column][value] += 1
            has_text = any(_missing_kind(row[indexes[column]]) is None for column in text_columns)
            has_link = any(_missing_kind(row[indexes[column]]) is None for column in link_columns)
            rows_with_text += int(has_text)
            rows_with_links += int(has_link)
            rows_with_text_and_links += int(has_text and has_link)

    expected_rows = int(manifest_entry.get("row_count", -1))
    if complete_scan and actual_rows != expected_rows:
        raise ValueError(f"{name}: row count differs from manifest ({actual_rows} != {expected_rows})")
    denominator = actual_rows
    return {
        "row_count": actual_rows,
        "manifest_row_count": expected_rows,
        "scan_complete": complete_scan,
        "bytes": size,
        "sha256": digest,
        "null_counts": _counter_json(null_counts),
        "blank_counts": _counter_json(blank_counts),
        "dimensions": {column: _counter_json(values) for column, values in dimensions.items() if values},
        "text": {
            "text_columns": text_columns,
            "link_columns": link_columns,
            "rows_with_text": rows_with_text,
            "rows_without_text": denominator - rows_with_text,
            "rows_with_links": rows_with_links,
            "rows_with_text_and_links": rows_with_text_and_links,
        }
        if text_columns
        else None,
    }


def _selected_entries(manifest: dict[str, Any], table_names: Iterable[str] | None) -> list[dict[str, Any]]:
    entries = manifest.get("tables")
    if not isinstance(entries, list) or not entries:
        raise ValueError("candidate manifest has no table inventory")
    by_name = {}
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            raise ValueError("candidate manifest has a malformed table entry")
        name = entry["name"]
        if name in by_name:
            raise ValueError(f"duplicate candidate table: {name}")
        by_name[name] = entry
    if table_names is None:
        return list(entries)
    names = list(table_names)
    if not names or len(set(names)) != len(names):
        raise ValueError("selected tables must be nonempty and unique")
    unknown = [name for name in names if name not in by_name]
    if unknown:
        raise ValueError(f"unknown candidate table(s): {', '.join(unknown)}")
    return [by_name[name] for name in names]


def evaluate_manifest(
    manifest_path: Path,
    *,
    table_names: Iterable[str] | None = None,
    max_rows_per_table: int | None = None,
) -> dict[str, Any]:
    """Evaluate one immutable candidate and return a JSON-serializable report."""

    if max_rows_per_table is not None and max_rows_per_table <= 0:
        raise ValueError("max_rows_per_table must be positive")
    manifest_path = manifest_path.expanduser()
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("candidate manifest is missing or symlinked")
    candidate_dir = manifest_path.parent
    if candidate_dir.is_symlink():
        raise ValueError("candidate directory is symlinked")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError("candidate manifest is not valid JSON") from error
    if not isinstance(manifest, dict):
        raise ValueError("candidate manifest must be an object")
    _, manifest_sha256 = _hash_file(manifest_path)
    serialization = manifest.get("serialization")
    null_marker = ""  # Legacy BICAM exports use PostgreSQL COPY CSV defaults.
    if serialization is not None:
        if not isinstance(serialization, dict):
            raise ValueError("candidate serialization must be an object")
        expected = {"format": "csv.gz", "encoding": "UTF-8", "newline": "LF", "header": True}
        if any(serialization.get(key) != value for key, value in expected.items()):
            raise ValueError("unsupported candidate CSV serialization")
        marker = serialization.get("csv_null")
        if not isinstance(marker, str) or not marker:
            raise ValueError("candidate serialization has no csv_null marker")
        null_marker = marker
    entries = _selected_entries(manifest, table_names)
    tables = {
        str(entry["name"]): _table_report(
            entry,
            candidate_dir,
            null_marker=null_marker,
            max_rows=max_rows_per_table,
        )
        for entry in entries
    }
    link_report: dict[str, Any] | None = None
    if "bills_version_links" in tables:
        link_entry = next(entry for entry in entries if entry["name"] == "bills_version_links")
        link_path = candidate_dir / str(link_entry["file"])
        status_counts: Counter[str] = Counter()
        rule_counts: Counter[str] = Counter()
        confidence_counts: Counter[str] = Counter()
        with gzip.open(link_path, "rt", encoding="utf-8", newline="") as stream:
            reader = _csv_rows(stream, null_marker)
            header = next(reader)
            for index, values in enumerate(reader, start=1):
                if max_rows_per_table is not None and index > max_rows_per_table:
                    break
                row = dict(zip(header, values, strict=True))
                for column, counter in (
                    ("link_status", status_counts),
                    ("link_rule", rule_counts),
                    ("link_confidence", confidence_counts),
                ):
                    value = row.get(column, "")
                    if _missing_kind(value) is None:
                        counter[value] += 1
        link_report = {
            "status": _counter_json(status_counts),
            "rule": _counter_json(rule_counts),
            "confidence": _counter_json(confidence_counts),
        }

    quality = manifest.get("build_evidence", {}).get("quality")
    return {
        "schema_version": 2,
        "evaluation": "offline-candidate-quality",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "candidate": str(manifest_path.resolve()),
        "manifest_sha256": manifest_sha256,
        "candidate_generated_at": manifest.get("generated_at"),
        "database_snapshot_at": manifest.get("database_snapshot_at"),
        "scope": manifest.get("scope"),
        "serialization": serialization,
        "null_semantics": "distinguishable",
        "csv_null_marker": null_marker,
        "serialization_basis": "manifest" if serialization is not None else "legacy_postgres_copy_csv_default",
        "complete_inventory_scan": len(tables) == len(manifest["tables"]) and all(t["scan_complete"] for t in tables.values()),
        "tables_scanned": list(tables),
        "table_count": len(tables),
        "tables": tables,
        "bill_version_links": link_report,
        "release_quality_evidence": quality,
        "limitations": [
            "Metrics are descriptive and do not establish population completeness.",
            "Source coverage is reported only when source columns are present in the public relation.",
            "Linkage precision/recall and manual text quality require an independently adjudicated corpus.",
            "A partial or row-bounded scan is not a complete release evaluation.",
            "Text availability counts nonblank raw_text/text; formatted_text is a rendition URL, not extracted content.",
            "Text availability does not establish extraction quality or classify individual fetch failures.",
        ],
    }


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# BICAM offline candidate-quality evaluation",
        "",
        f"- Candidate: `{report['candidate']}`",
        f"- Manifest SHA-256: `{report['manifest_sha256']}`",
        f"- Candidate snapshot: `{report.get('database_snapshot_at')}`",
        f"- Tables scanned: {report['table_count']}",
        f"- Complete inventory scan: {report['complete_inventory_scan']}",
        f"- NULL semantics: **{report['null_semantics']}**",
        "",
        "This is a descriptive, offline scan. It is not a population-completeness,",
        "linkage-accuracy, or manual text-quality claim.",
        "",
        "| Relation | Rows | Text rows | Rows with links | Scan complete |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for name, table in report["tables"].items():
        text = table.get("text") or {}
        text_count = text.get("rows_with_text")
        link_count = text.get("rows_with_links")
        text_display = f"{text_count:,}" if isinstance(text_count, int) else "—"
        link_display = f"{link_count:,}" if isinstance(link_count, int) else "—"
        lines.append(
            f"| `{name}` | {table['row_count']:,} | {text_display} "
            f"| {link_display} | {table['scan_complete']} |"
        )
    links = report.get("bill_version_links")
    if links is not None:
        lines.extend(["", "## Bill-version links", "", f"- Status: `{json.dumps(links['status'], sort_keys=True)}`", f"- Rules: `{json.dumps(links['rule'], sort_keys=True)}`", f"- Confidence: `{json.dumps(links['confidence'], sort_keys=True)}`"])
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in report["limitations"])
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, help="JSON report path (Markdown is written beside it).")
    parser.add_argument("--table", dest="tables", action="append", help="Scan only this relation; repeatable.")
    parser.add_argument("--max-rows-per-table", type=int, help="Bound each table scan for a representative evaluation.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.max_rows_per_table is not None and args.max_rows_per_table <= 0:
        raise SystemExit("--max-rows-per-table must be positive")
    candidate_dir = args.manifest.expanduser().parent.resolve()
    output = (args.output or candidate_dir.parent / "quality-reports" / f"{candidate_dir.name}.json").expanduser()
    for destination in (output, output.with_suffix(".md")):
        if destination.resolve().is_relative_to(candidate_dir):
            raise ValueError("evaluation output must be outside the immutable candidate")
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(f"evaluation output already exists: {destination}")
    report = evaluate_manifest(
        args.manifest,
        table_names=args.tables,
        max_rows_per_table=args.max_rows_per_table,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(report, indent=2, sort_keys=True) + "\n")
    with output.with_suffix(".md").open("x", encoding="utf-8") as stream:
        stream.write(_markdown(report))
    print(json.dumps({"output": str(output), "tables": report["table_count"], "null_semantics": report["null_semantics"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
