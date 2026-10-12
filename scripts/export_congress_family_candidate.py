"""Offline all-family Congress candidate with explicit reference closure.

Preserves the original frozen snapshot. This creates a candidate, not a claim of
complete source coverage or release acceptance. Unresolved issues stay visible.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import csv
import gzip
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as acsv
import pyarrow.parquet as pq
import yaml

from bicam_collection.export_scope import primary_root_selected, in_congress
from bicam_collection.export_semantics import normalize_value
from export_bills_parquet import TYPES, value, digest_rows, sha
from project_amendment_export_repairs import canonical_row


INDIRECT = {"bills_reference_codes_sections": ("bill_code_id", "bills_reference_codes"),
            "bills_reference_statutes_pages": ("bill_statute_id", "bills_reference_statutes")}


class Candidate:
    def __init__(self, snapshot, output, contract_path, congress, hearing_additions, child_additions=None,
        amendment_repairs=None, include_supplemental=False):
        self.snapshot, self.output, self.congress = snapshot, output, congress
        self.manifest_hash = sha(snapshot / "manifest.json")
        self.manifest = json.loads((snapshot / "manifest.json").read_text())
        if self.manifest.get("serialization", {}).get("csv_null") != r"\N":
            raise ValueError("explicit source NULL serialization required")
        self.contract_hash = sha(contract_path)
        contract = yaml.safe_load(contract_path.read_text())
        self.families = contract["families"]
        self.roots = {f["name"]: f for f in self.families}
        allowed_statuses = {"core-now"}
        if include_supplemental:
            allowed_statuses.add("supplemental")
        family_relations = [r for f in self.families for r in [f, *f.get("children", [])]]
        supplemental_relations = contract.get("supplemental_relations", [])
        self.relations = {r["table"]: r for r in [*family_relations, *supplemental_relations]
                          if r["feature_status"] in allowed_statuses}
        self.supplemental_names = {r["table"] for r in supplemental_relations
                                   if r["feature_status"] in allowed_statuses}
        self.inventory = {t["name"]: t for t in self.manifest["tables"]}
        self.primary, self.references, self.available = defaultdict(set), defaultdict(set), defaultdict(set)
        self.indirect_keys, self.exported_values = defaultdict(set), defaultdict(set)
        self.verified, self.records, self.issues, self.normalizations = set(), [], [], Counter()
        self.additions = []
        self.additions_hash = None
        self.child_additions = {}
        self.child_additions_hash = None
        self.amendment_repairs = {}
        self.amendment_repairs_hash = None
        self.amendment_temporal_scope = None
        self.amendment_repair_exceptions = []
        self.applied_amendments = set()
        if child_additions:
            evidence = json.loads(child_additions.read_text())
            if evidence["source_manifest_sha256"] != self.manifest_hash:
                raise ValueError("child supplement belongs to another frozen source snapshot")
            for name, entries in evidence["additions"].items():
                if name not in {"hearings_metadata", "hearings_metadata_granules", "hearings_dates",
                                "hearings_committees", "hearings_members", "hearings_witnesses", "hearings_bills"}:
                    raise ValueError("unreviewed child supplement relation")
                self.child_additions[name] = [entry["row"] for entry in entries]
            self.child_additions_hash = sha(child_additions)
        if amendment_repairs:
            evidence = json.loads(amendment_repairs.read_text())
            if evidence["source_manifest_sha256"] != self.manifest_hash or evidence["contract_sha256"] != self.contract_hash:
                raise ValueError("amendment supplement belongs to another source snapshot or contract")
            if congress != 119 or not 0 < len(evidence["root_replacements"]) <= 250:
                raise ValueError("amendment supplement outside reviewed Congress-119 bounds")
            for entry in evidence["root_replacements"]:
                identity = entry["expected"]["amendment_id"]
                if identity in self.amendment_repairs or entry["row"]["amendment_id"] != identity:
                    raise ValueError("amendment supplement duplicates or changes identity")
                self.amendment_repairs[identity] = entry
            for name, rows in evidence["child_additions"].items():
                if not name.startswith("amendments_") or name not in self.relations:
                    raise ValueError("unreviewed amendment supplement relation")
                if any(set(row) != set(self.relations[name]["columns"]) for row in rows):
                    raise ValueError("amendment child columns differ from contract")
                self.child_additions[name] = rows
            self.amendment_repairs_hash = sha(amendment_repairs)
            self.amendment_temporal_scope = evidence["temporal_scope"]
            self.amendment_repair_exceptions = evidence["exceptions"]
        if hearing_additions:
            evidence = json.loads(hearing_additions.read_text())
            if evidence["manifest_sha256"] != self.manifest_hash:
                raise ValueError("hearing source supplement is for a different frozen snapshot")
            self.additions = [a["row"] for a in evidence["proposed_additions"]]
            self.additions_hash = sha(hearing_additions)
        self.output.mkdir(parents=True, exist_ok=False)

    def rows(self, name):
        relation, entry = self.relations[name], self.inventory[name]
        path = self.snapshot / entry["file"]
        if name not in self.verified:
            if path.is_symlink() or path.parent.resolve() != self.snapshot.resolve() or sha(path) != entry["sha256"]:
                raise ValueError(f"source path/hash mismatch: {name}")
            with gzip.open(path, "rt", newline="") as stream:
                if next(csv.reader(stream)) != list(relation["columns"]):
                    raise ValueError(f"source header differs: {name}")
        columns = [c for c in relation["columns"] if c != "raw_text"]
        n = 0
        with acsv.open_csv(path, read_options=acsv.ReadOptions(block_size=32 * 1024 * 1024, use_threads=False),
            parse_options=acsv.ParseOptions(newlines_in_values=True),
            convert_options=acsv.ConvertOptions(include_columns=columns,
                column_types={c: pa.string() for c in columns}, null_values=[r"\N"],
                strings_can_be_null=True, quoted_strings_can_be_null=False)) as reader:
            for batch in reader:
                n += batch.num_rows
                yield from batch.to_pylist()
        if n != entry["row_count"]:
            raise ValueError(f"source row count differs: {name}")
        self.verified.add(name)

    def discover_roots(self):
        for row in self.rows("members_terms"):
            if in_congress(row.get("congress"), self.congress):
                self.primary["members"].add(row["bioguide_id"])
        for family, spec in self.roots.items():
            key = spec["primary_key"][0]
            for row in self.rows(family):
                self.available[family].add(row[key])
                if primary_root_selected(family, row, self.congress, serving_members=self.primary["members"]):
                    self.primary[family].add(row[key])
        # Congressional Directory membership is itself a first-class 119th
        # relationship.  It must be allowed to pull a committee (and member)
        # into the family closure even when that committee has no bill/report
        # edge in the selected Congress.
        if "committees_members" in self.relations:
            for row in self.rows("committees_members"):
                if in_congress(row.get("congress"), self.congress):
                    if row.get("committee_code"):
                        self.primary["committees"].add(row["committee_code"])
                    if row.get("bioguide_id"):
                        self.primary["members"].add(row["bioguide_id"])
        for row in self.additions:
            key = row["hearing_id"]
            if key in self.available["hearings"]:
                raise ValueError("source supplement would overwrite an existing hearing")
            self.available["hearings"].add(key)
            if primary_root_selected("hearings", row, self.congress):
                self.primary["hearings"].add(key)

    def references_from(self, relation, row):
        for column, target in relation.get("foreign_keys", {}).items():
            family, _ = target.split(".")
            if family in self.roots and row.get(column) is not None:
                self.references[family].add(str(row[column]))
                if family == "committees":
                    self.primary[family].add(str(row[column]))
        for column in ("congress", "congress_number", "congress_received", "congress_considered"):
            if row.get(column) is not None:
                self.references["congresses"].add(str(row[column]))

    def selected_child(self, family, name, row):
        owner = self.roots[family]["primary_key"][0]
        if name in INDIRECT:
            column, parent = INDIRECT[name]
            selected = row.get(column) in self.indirect_keys[parent]
        else:
            selected = row.get(owner) in self.primary[family]
        if not selected:
            return False
        if "congress" in row and not in_congress(row["congress"], self.congress):
            return False
        if family == "committees":
            if name == "committees_bills":
                return row.get("bill_id") in self.primary["bills"]
            if name == "committees_committeereports":
                return row.get("report_id") in self.primary["committee_reports"]
            if name == "committees_subcommittees":
                return row.get("subcommittee_code") in self.primary["committees"]
        return True

    def child_rows(self, family, name):
        for row in self.rows(name):
            if self.selected_child(family, name, row):
                yield row
        for row in self.child_additions.get(name, []):
            if self.selected_child(family, name, row):
                yield row

    def repair_root(self, family, row):
        if family != "amendments" or row["amendment_id"] not in self.amendment_repairs:
            return row
        identity = row["amendment_id"]
        entry = self.amendment_repairs[identity]
        columns = self.relations[family]["columns"]
        if canonical_row(columns, row) != entry["expected"]:
            raise ValueError("amendment root no longer matches the reviewed placeholder")
        if set(entry["row"]) != set(columns):
            raise ValueError("amendment root columns differ from contract")
        self.applied_amendments.add(identity)
        return entry["row"]

    def write(self, name, input_rows):
        relation = self.relations[name]
        columns = relation["columns"]
        schema = pa.schema([(c, TYPES[t]) for c, t in columns.items()])
        path = self.output / f"{name}.parquet"
        digest, seen, count, pending = hashlib.sha256(), set(), 0, []
        nulls = Counter()
        unique = self.inventory[name].get("key_semantics", "unique") == "unique"
        pk = relation.get("primary_key", [])
        duplicate_count = 0
        def flush(writer):
            nonlocal pending
            if pending:
                table = pa.Table.from_pylist(pending, schema=schema)
                digest_rows(digest, table.to_pylist())
                writer.write_table(table, row_group_size=8192)
                pending = []
        with pq.ParquetWriter(path, schema, compression="zstd") as writer:
            for row in input_rows:
                self.references_from(relation, row)
                typed = {}
                for column, kind in columns.items():
                    raw = None if column == "raw_text" else row.get(column)
                    raw = raw if raw is None or isinstance(raw, str) else str(raw).lower() if isinstance(raw, bool) else str(raw)
                    normalized, reason = normalize_value(name, column, raw)
                    if normalized != raw:
                        self.normalizations[(name, column, str(raw), str(normalized))] += 1
                    if reason:
                        self.issues.append({"table": name, "column": column, "value": raw, "reason": reason})
                    typed[column] = value(normalized, kind)
                    if typed[column] is None:
                        nulls[column] += 1
                if unique and pk:
                    key = tuple(typed[c] for c in pk)
                    if key in seen:
                        duplicate_count += 1
                    seen.add(key)
                for child_name, (column, parent) in INDIRECT.items():
                    if name == parent:
                        self.indirect_keys[name].add(row[column])
                # All declared target columns are tracked for complete selected FK checks.
                for target_column in self.target_columns.get(name, ()):
                    if typed[target_column] is not None:
                        self.exported_values[(name, target_column)].add(str(typed[target_column]))
                pending.append(typed)
                count += 1
                if len(pending) >= 4096:
                    flush(writer)
            flush(writer)
        if duplicate_count:
            self.issues.append({"table": name, "reason": "duplicate_normalized_primary_key", "rows": duplicate_count})
        observed = hashlib.sha256()
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=4096):
            digest_rows(observed, batch.to_pylist())
        if parquet.metadata.num_rows != count or observed.digest() != digest.digest():
            raise ValueError(f"typed Parquet roundtrip mismatch: {name}")
        self.records.append({"name": name, "file": path.name, "row_count": count, "sha256": sha(path),
            "bytes": path.stat().st_size, "typed_rows_sha256": digest.hexdigest(),
            "primary_key": pk, "key_semantics": self.inventory[name].get("key_semantics", "unique"),
            "foreign_keys": relation.get("foreign_keys", {}), "null_counts": dict(nulls),
            "columns": [{"name": c, "postgres_type": t} for c, t in columns.items()]})
        print(json.dumps({"table": name, "rows": count}), flush=True)

    def run(self):
        self.target_columns = defaultdict(set)
        for relation in self.relations.values():
            for target in relation.get("foreign_keys", {}).values():
                name, column = target.split(".")
                self.target_columns[name].add(column)
        self.discover_roots()
        for family, spec in self.roots.items():
            if family == "committees":
                continue
            for child in spec.get("children", []):
                name = child["table"]
                if name not in self.relations:
                    continue
                self.write(name, self.child_rows(family, name))
        # Resolve committee hierarchy before exporting the committee family.
        hierarchy = list(self.rows("committees_subcommittees"))
        while True:
            parents = {r["committee_code"] for r in hierarchy if r["subcommittee_code"] in self.primary["committees"]}
            additions = parents - self.primary["committees"]
            if not additions:
                break
            self.primary["committees"].update(additions)
        for child in self.roots["committees"]["children"]:
            name = child["table"]
            self.write(name, (r for r in self.rows(name) if self.selected_child("committees", name, r)))
        for name in self.supplemental_names:
            self.write(name, (r for r in self.rows(name) if r.get("bill_id") in self.primary["bills"]))
        # Congress dependency rows are selected after all other root fields are seen.
        order = [f for f in self.roots if f != "congresses"] + ["congresses"]
        inclusion = {}
        for family in order:
            key = self.roots[family]["primary_key"][0]
            wanted = self.primary[family] | self.references[family]
            absent = wanted - self.available[family]
            if absent:
                self.issues.append({"table": family, "reason": "required_root_missing", "ids": sorted(absent)})
            def roots_to_write():
                for row in self.rows(family):
                    if row[key] in wanted:
                        yield self.repair_root(family, row)
                if family == "hearings":
                    yield from (r for r in self.additions if r[key] in wanted)
            self.write(family, roots_to_write())
            inclusion[family] = {"primary": sorted(self.primary[family]),
                                 "dependency_only": sorted(self.references[family] - self.primary[family])}
        # Check every declared selected foreign key, including non-root parents.
        for record in self.records:
            if not record["foreign_keys"]:
                continue
            counts = Counter()
            for batch in pq.ParquetFile(self.output / record["file"]).iter_batches(batch_size=4096):
                for row in batch.to_pylist():
                    for field, target in record["foreign_keys"].items():
                        if row[field] is not None and str(row[field]) not in self.exported_values[tuple(target.split("."))]:
                            counts[(field, target)] += 1
            for (field, target), count in counts.items():
                self.issues.append({"table": record["name"], "column": field, "target": target,
                                    "reason": "selected_foreign_key_missing", "rows": count})
        if self.verified != self.relations.keys() or sha(self.snapshot / "manifest.json") != self.manifest_hash:
            raise ValueError("incomplete or changed source inventory")
        if self.applied_amendments != self.amendment_repairs.keys():
            raise ValueError("not every amendment root repair was applied")
        records = {r["name"]: r for r in self.records}
        result = {"schema_version": 1, "status": "candidate_not_release_accepted", "congress": self.congress,
            "source_snapshot": self.snapshot.name, "source_manifest_sha256": self.manifest_hash,
            "database_snapshot_at": self.manifest["database_snapshot_at"], "contract_sha256": self.contract_hash,
            "hearing_parent_supplement_sha256": self.additions_hash, "hearing_parents_added": len(self.additions),
            "child_supplement_sha256": self.child_additions_hash,
            "child_additions": {k: len(v) for k, v in self.child_additions.items()},
            "amendment_supplement_sha256": self.amendment_repairs_hash,
            "amendment_roots_repaired": len(self.applied_amendments),
            "supplemental_observation_scope": self.amendment_temporal_scope,
            "amendment_repair_exceptions": self.amendment_repair_exceptions,
            "created_at": datetime.now(timezone.utc).isoformat(), "tables": [records[n] for n in self.relations],
            "root_counts": {f: {k: len(v) for k, v in reasons.items()} for f, reasons in inclusion.items()},
            "normalizations": [{"table": t, "column": c, "from": old, "to": new, "rows": n}
                               for (t, c, old, new), n in self.normalizations.items()],
            "structural_issues": self.issues, "release_ready": False,
            "remaining_acceptance": ["original non-document metadata feature gaps", "source completeness and placeholder metadata review",
                "full semantic domain/identifier review beyond declared normalization rules", "PostgreSQL roundtrip"],
            "omissions": {"bills_lobbied": "external crosswalk unavailable", "raw_text": "document bodies intentionally NULL; columns retained"}}
        (self.output / "inclusion.json").write_text(json.dumps(inclusion, indent=2) + "\n")
        result["inclusion_sha256"] = sha(self.output / "inclusion.json")
        (self.output / "manifest.json").write_text(json.dumps(result, indent=2) + "\n")
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=Path("contracts/bicam_public_schema.yaml"))
    parser.add_argument("--congress", type=int, default=119)
    parser.add_argument("--hearing-additions", type=Path)
    parser.add_argument("--child-additions", type=Path)
    parser.add_argument("--amendment-repairs", type=Path)
    parser.add_argument("--include-supplemental", action="store_true",
                        help="Include declared supplemental relations such as bills_versions.")
    args = parser.parse_args()
    Candidate(args.snapshot, args.output, args.contract, args.congress, args.hearing_additions,
              args.child_additions, args.amendment_repairs, args.include_supplemental).run()
