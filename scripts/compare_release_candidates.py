"""Diff two timestamped export candidates (never ``latest``).

Verifies the declared files, streams each CSV through a temporary SQLite index,
and reports added, removed and changed rows, duplicate keys, and per-Congress
count deltas. Examples show keys, hashes and short previews only.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from typing import Any, Iterable, Iterator, Mapping


CSV_FIELD_SIZE_LIMIT = 1024 * 1024 * 1024
DEFAULT_MAX_EXAMPLES = 20
_PREVIEW_COLUMNS = 6
_PREVIEW_VALUE_LIMIT = 180


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def _candidate_root(manifest_path: Path) -> tuple[Path, dict[str, Any]]:
    """Read one exact candidate and reject mutable/symlinked paths."""

    manifest_path = manifest_path.expanduser()
    if manifest_path.name != "manifest.json":
        raise ValueError(f"candidate must be named manifest.json: {manifest_path}")
    if manifest_path.is_symlink() or manifest_path.parent.is_symlink():
        raise ValueError(f"candidate manifest and directory must not be symlinks: {manifest_path}")
    candidate_dir = manifest_path.parent
    if not candidate_dir.is_dir():
        raise ValueError(f"candidate directory does not exist: {candidate_dir}")
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"candidate manifest is not valid JSON: {manifest_path}") from error
    if not isinstance(document, dict):
        raise ValueError(f"candidate manifest must be an object: {manifest_path}")
    if document.get("snapshot_directory") != candidate_dir.name:
        raise ValueError(f"candidate snapshot_directory does not match {candidate_dir}")
    tables = document.get("tables")
    if not isinstance(tables, list) or not tables:
        raise ValueError(f"candidate has no table inventory: {manifest_path}")
    names: set[str] = set()
    for table in tables:
        if not isinstance(table, dict) or not isinstance(table.get("name"), str):
            raise ValueError(f"candidate has an invalid table entry: {manifest_path}")
        name = str(table["name"])
        if name in names:
            raise ValueError(f"candidate has a duplicate relation: {name}")
        names.add(name)
        file_name = table.get("file")
        if file_name != f"{name}.csv.gz" or Path(str(file_name)).name != file_name:
            raise ValueError(f"{name}: unsafe or unexpected artifact filename {file_name!r}")
        artifact = candidate_dir / str(file_name)
        if not artifact.is_file() or artifact.is_symlink():
            raise ValueError(f"{name}: artifact is missing or symlinked")
    return candidate_dir, document


def _key_columns(table: Mapping[str, Any]) -> tuple[tuple[str, ...], str]:
    primary = table.get("primary_key")
    if isinstance(primary, list) and primary and all(isinstance(item, str) for item in primary):
        return tuple(primary), "unique"
    grouping = table.get("grouping_key")
    if isinstance(grouping, list) and grouping and all(isinstance(item, str) for item in grouping):
        return tuple(grouping), "grouping"
    return (), "row"


def _key_payload(row: Mapping[str, str], columns: tuple[str, ...]) -> str:
    if columns:
        return _json([row.get(column) for column in columns])
    return _json(row)


def _preview(row: Mapping[str, str], columns: list[str]) -> str:
    values: dict[str, str] = {}
    for column in columns[:_PREVIEW_COLUMNS]:
        value = row.get(column, "")
        values[column] = value if len(value) <= _PREVIEW_VALUE_LIMIT else value[:_PREVIEW_VALUE_LIMIT] + "…"
    return _json(values)


def _display_key(value: str) -> object:
    """Decode structured keys while retaining opaque row-digest keys."""

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _read_csv_rows(path: Path) -> tuple[list[str], Iterator[dict[str, str]]]:
    """Return a header and lazy row iterator for one gzip CSV artifact."""

    csv.field_size_limit(CSV_FIELD_SIZE_LIMIT)
    stream = gzip.open(path, "rt", encoding="utf-8", newline="")
    reader = csv.reader(stream)
    try:
        header = next(reader)
    except StopIteration as error:
        stream.close()
        raise ValueError(f"empty CSV artifact: {path}") from error
    if not header or len(header) != len(set(header)):
        stream.close()
        raise ValueError(f"CSV header is empty or duplicated: {path}")

    def rows() -> Iterator[dict[str, str]]:
        try:
            for values in reader:
                if len(values) != len(header):
                    raise ValueError(f"CSV row width differs from header: {path}")
                yield dict(zip(header, values, strict=True))
        finally:
            stream.close()

    return header, rows()


def _manifest_table_map(document: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    tables = document.get("tables")
    if not isinstance(tables, list):
        raise ValueError("candidate tables must be a list")
    return {str(table["name"]): table for table in tables if isinstance(table, dict)}


def _insert_candidate(
    connection: sqlite3.Connection,
    *,
    label: str,
    candidate_dir: Path,
    table: Mapping[str, Any],
    key_columns_override: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    name = str(table["name"])
    path = candidate_dir / str(table["file"])
    size, digest = _hash_file(path)
    header, rows = _read_csv_rows(path)
    # ``columns`` is in CSV order; ``contract_columns`` is a type map in YAML order.
    manifest_columns = table.get("columns")
    declared_header = (
        [str(column["name"]) for column in manifest_columns if isinstance(column, dict) and "name" in column]
        if isinstance(manifest_columns, list)
        else None
    )
    header_match = declared_header is None or header == declared_header
    declared_key_columns, declared_key_semantics = _key_columns(table)
    key_columns = declared_key_columns if key_columns_override is None else key_columns_override
    key_semantics = declared_key_semantics if key_columns_override is None else "row"
    missing_keys = [column for column in declared_key_columns if column not in header]
    if missing_keys:
        raise ValueError(f"{name}: key columns missing from CSV header: {missing_keys}")
    congress_column = "congress" if "congress" in header else None
    row_count = 0
    for row in rows:
        row_json = _json(row)
        row_hash = hashlib.sha256(row_json.encode("utf-8")).hexdigest()
        # Without a trustworthy key, index by row digest rather than storing whole rows.
        key = row_hash if not key_columns else _key_payload(row, key_columns)
        congress = row.get(congress_column) if congress_column else None
        connection.execute(
            "insert into observations(dataset, relation, key_json, row_hash, congress, preview) values (?, ?, ?, ?, ?, ?)",
            (label, name, key, row_hash, congress, _preview(row, header)),
        )
        row_count += 1
    return {
        "name": name,
        "header": header,
        "header_matches_contract": header_match,
        "key_columns": list(key_columns),
        "key_semantics": key_semantics,
        "declared_key_columns": list(declared_key_columns),
        "declared_key_semantics": declared_key_semantics,
        "row_count": row_count,
        "declared_row_count": table.get("row_count"),
        "row_count_matches_manifest": table.get("row_count") in (None, row_count),
        "bytes": size,
        "declared_bytes": table.get("bytes"),
        "bytes_match_manifest": table.get("bytes") in (None, size),
        "sha256": digest,
        "declared_sha256": table.get("sha256"),
        "hash_matches_manifest": table.get("sha256") in (None, digest),
    }


def _grouped(
    connection: sqlite3.Connection,
    label: str,
    relation: str,
    congress: str | None = None,
) -> Iterator[tuple[str, dict[str, int], list[dict[str, str]]]]:
    query = (
        "select key_json, row_hash, count(*) as count, max(preview) as preview "
        "from observations where dataset = ? and relation = ?"
    )
    parameters: list[object] = [label, relation]
    if congress is not None:
        query += " and congress = ?"
        parameters.append(congress)
    query += " group by key_json, row_hash order by key_json, row_hash"
    current_key: str | None = None
    hashes: dict[str, int] = {}
    previews: list[dict[str, str]] = []
    for key, row_hash, count, preview in connection.execute(query, parameters):
        key = str(key)
        if current_key is not None and key != current_key:
            yield current_key, hashes, previews
            hashes = {}
            previews = []
        current_key = key
        hashes[str(row_hash)] = int(count)
        if preview:
            previews.append({"row_hash": str(row_hash), "preview": str(preview)})
    if current_key is not None:
        yield current_key, hashes, previews


def _merge_groups(
    before: Iterator[tuple[str, dict[str, int], list[dict[str, str]]]],
    after: Iterator[tuple[str, dict[str, int], list[dict[str, str]]]],
    *,
    max_examples: int,
) -> dict[str, Any]:
    before_item = next(before, None)
    after_item = next(after, None)
    added_rows = removed_rows = retained_rows = changed_keys = 0
    examples: dict[str, list[dict[str, Any]]] = {"added": [], "removed": [], "changed": []}
    while before_item is not None or after_item is not None:
        if after_item is None or (before_item is not None and before_item[0] < after_item[0]):
            key, hashes, previews = before_item  # type: ignore[misc]
            count = sum(hashes.values())
            removed_rows += count
            if len(examples["removed"]) < max_examples:
                examples["removed"].append({"key": _display_key(key), "before_hashes": hashes, "before_preview": previews[:1]})
            before_item = next(before, None)
            continue
        if before_item is None or after_item[0] < before_item[0]:
            key, hashes, previews = after_item
            count = sum(hashes.values())
            added_rows += count
            if len(examples["added"]) < max_examples:
                examples["added"].append({"key": _display_key(key), "after_hashes": hashes, "after_preview": previews[:1]})
            after_item = next(after, None)
            continue
        key = before_item[0]
        before_hashes, after_hashes = before_item[1], after_item[1]
        common = set(before_hashes) & set(after_hashes)
        retained_rows += sum(min(before_hashes[row_hash], after_hashes[row_hash]) for row_hash in common)
        removed_for_key = sum(max(before_hashes.get(row_hash, 0) - after_hashes.get(row_hash, 0), 0) for row_hash in before_hashes)
        added_for_key = sum(max(after_hashes.get(row_hash, 0) - before_hashes.get(row_hash, 0), 0) for row_hash in after_hashes)
        removed_rows += removed_for_key
        added_rows += added_for_key
        if before_hashes != after_hashes:
            changed_keys += 1
            if len(examples["changed"]) < max_examples:
                examples["changed"].append(
                    {
                        "key": _display_key(key),
                        "before_hashes": before_hashes,
                        "after_hashes": after_hashes,
                        "before_preview": before_item[2][:1],
                        "after_preview": after_item[2][:1],
                    }
                )
        before_item = next(before, None)
        after_item = next(after, None)
    return {
        "added_rows": added_rows,
        "removed_rows": removed_rows,
        "retained_rows": retained_rows,
        "changed_keys": changed_keys,
        "examples": examples,
    }


def _duplicate_keys(connection: sqlite3.Connection, label: str, relation: str, max_examples: int) -> dict[str, Any]:
    rows = connection.execute(
        "select key_json, count(*) from observations where dataset = ? and relation = ? "
        "group by key_json having count(*) > 1 order by key_json limit ?",
        (label, relation, max_examples),
    ).fetchall()
    total = connection.execute(
        "select count(*) from (select key_json from observations where dataset = ? and relation = ? "
        "group by key_json having count(*) > 1)",
        (label, relation),
    ).fetchone()[0]
    return {"count": int(total), "examples": [{"key": _display_key(str(key)), "count": int(count)} for key, count in rows]}


def _congress_counts(connection: sqlite3.Connection, relation: str) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for label in ("before", "after"):
        rows = connection.execute(
            "select coalesce(congress, '<null>'), count(*), count(distinct key_json) "
            "from observations where dataset = ? and relation = ? group by coalesce(congress, '<null>') order by 1",
            (label, relation),
        ).fetchall()
        for congress, row_count, key_count in rows:
            result.setdefault(str(congress), {})[f"{label}_rows"] = int(row_count)
            result[str(congress)][f"{label}_keys"] = int(key_count)
    for values in result.values():
        for field in ("before_rows", "after_rows", "before_keys", "after_keys"):
            values.setdefault(field, 0)
        values["row_delta"] = values["after_rows"] - values["before_rows"]
        values["key_delta"] = values["after_keys"] - values["before_keys"]
    return result


def compare_candidates(
    before_manifest: Path,
    after_manifest: Path,
    *,
    tables: Iterable[str] | None = None,
    max_examples: int = DEFAULT_MAX_EXAMPLES,
) -> dict[str, Any]:
    """Compare two exact candidate directories using a temporary SQLite index."""

    if max_examples < 0:
        raise ValueError("max_examples must be non-negative")
    before_dir, before_document = _candidate_root(before_manifest)
    after_dir, after_document = _candidate_root(after_manifest)
    before_tables = _manifest_table_map(before_document)
    after_tables = _manifest_table_map(after_document)
    requested = list(tables) if tables is not None else sorted(set(before_tables) | set(after_tables))
    unknown = [name for name in requested if name not in before_tables or name not in after_tables]
    if unknown:
        raise ValueError(f"relation is missing from one candidate: {', '.join(unknown)}")
    if len(requested) != len(set(requested)):
        raise ValueError("duplicate relation selection")
    with tempfile.TemporaryDirectory(prefix="bicam-candidate-compare-") as temporary:
        database = Path(temporary) / "observations.sqlite"
        connection = sqlite3.connect(database)
        connection.execute(
            "create table observations(dataset text not null, relation text not null, key_json text not null, "
            "row_hash text not null, congress text, preview text)"
        )
        connection.execute("create index observations_lookup on observations(dataset, relation, key_json)")
        connection.execute("create index observations_congress on observations(dataset, relation, congress)")
        table_reports: list[dict[str, Any]] = []
        try:
            for name in requested:
                before_keys, before_semantics = _key_columns(before_tables[name])
                after_keys, after_semantics = _key_columns(after_tables[name])
                same_key_contract = before_keys == after_keys and before_semantics == after_semantics
                comparison_keys = before_keys if same_key_contract else ()
                before_info = _insert_candidate(
                    connection,
                    label="before",
                    candidate_dir=before_dir,
                    table=before_tables[name],
                    key_columns_override=comparison_keys,
                )
                after_info = _insert_candidate(
                    connection,
                    label="after",
                    candidate_dir=after_dir,
                    table=after_tables[name],
                    key_columns_override=comparison_keys,
                )
                connection.commit()
                # Key contract changed: compare whole rows and report the change.
                diff = _merge_groups(
                    _grouped(connection, "before", name),
                    _grouped(connection, "after", name),
                    max_examples=max_examples,
                )
                table_reports.append(
                    {
                        "table": name,
                        "before": before_info,
                        "after": after_info,
                        "key_contract": {
                            "same": same_key_contract,
                            "before_columns": list(before_keys),
                            "after_columns": list(after_keys),
                            "before_semantics": before_semantics,
                            "after_semantics": after_semantics,
                        },
                        "duplicate_keys": {
                            "before": _duplicate_keys(connection, "before", name, max_examples),
                            "after": _duplicate_keys(connection, "after", name, max_examples),
                        },
                        "diff": diff,
                        "by_congress": _congress_counts(connection, name),
                    }
                )
        finally:
            connection.close()
    before_names = set(before_tables)
    after_names = set(after_tables)
    return {
        "schema_version": 1,
        "comparison": "immutable_candidate_before_after",
        "before": {
            "manifest": str(Path(before_manifest).expanduser().resolve()),
            "snapshot_directory": before_dir.name,
            "contract_version": before_document.get("contract_version"),
            "manifest_sha256": _hash_file(Path(before_manifest).expanduser())[1],
            "relation_count": len(before_names),
        },
        "after": {
            "manifest": str(Path(after_manifest).expanduser().resolve()),
            "snapshot_directory": after_dir.name,
            "contract_version": after_document.get("contract_version"),
            "manifest_sha256": _hash_file(Path(after_manifest).expanduser())[1],
            "relation_count": len(after_names),
        },
        "inventory": {
            "exact": before_names == after_names,
            "missing_after": sorted(before_names - after_names),
            "added_after": sorted(after_names - before_names),
            "tables_compared": requested,
        },
        "tables": table_reports,
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# BICAM release candidate comparison",
        "",
        f"- Before: `{report['before']['snapshot_directory']}` (contract {report['before']['contract_version']})",
        f"- After: `{report['after']['snapshot_directory']}` (contract {report['after']['contract_version']})",
        f"- Exact relation inventory: {'yes' if report['inventory']['exact'] else 'no'}",
        "",
        "| Table | Before | After | Added rows | Removed rows | Changed keys | Duplicate keys before/after |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for table in report["tables"]:
        before = table["before"]
        after = table["after"]
        diff = table["diff"]
        duplicates = table["duplicate_keys"]
        lines.append(
            f"| `{table['table']}` | {before['row_count']} | {after['row_count']} | "
            f"{diff['added_rows']} | {diff['removed_rows']} | {diff['changed_keys']} | "
            f"{duplicates['before']['count']} / {duplicates['after']['count']} |"
        )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True, help="Exact before candidate manifest.json")
    parser.add_argument("--after", type=Path, required=True, help="Exact after candidate manifest.json")
    parser.add_argument("--tables", nargs="+", help="Compare only these relations (default: all shared relations)")
    parser.add_argument("--max-examples", type=int, default=DEFAULT_MAX_EXAMPLES)
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--output", type=Path, help="Write the rendered report to this path")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = compare_candidates(
            args.before,
            args.after,
            tables=args.tables,
            max_examples=args.max_examples,
        )
    except (OSError, ValueError, csv.Error, gzip.BadGzipFile, sqlite3.Error) as error:
        print(f"candidate comparison failed: {error}", file=sys.stderr)
        return 1
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n" if args.format == "json" else render_markdown(report)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
