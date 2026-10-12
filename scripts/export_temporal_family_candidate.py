"""Build a date-bounded all-family BICAM Parquet candidate.

The cutoff is applied only to explicit source dates/timestamps.  Rows without
such a field are included when they preserve the selected parent/foreign-key
closure (for example bill titles, member terms, metadata, and committee
membership).  Parent rows pulled in solely to satisfy a selected child are
marked as dependency-only in ``inclusion.json`` by the inherited candidate
writer; no fetch or source date is invented.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path

from bicam_collection.export_semantics import parse_source_timestamp
from export_congress_family_candidate import Candidate


def parse_temporal(value: object, kind: str) -> datetime | date | None:
    if value in (None, ""):
        return None
    if kind == "date":
        return date.fromisoformat(str(value)[:10])
    if kind == "timestamptz":
        return parse_source_timestamp(str(value))
    return None


class TemporalCandidate(Candidate):
    def __init__(self, snapshot: Path, output: Path, contract: Path, since: datetime,
                 as_of: datetime | None = None, include_supplemental: bool = False):
        super().__init__(snapshot, output, contract, 119, None,
                         include_supplemental=include_supplemental)
        if since.tzinfo is None:
            raise ValueError("cutoff must include timezone")
        self.since = since.astimezone(timezone.utc)
        self.as_of = (as_of or datetime.max.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
        self.date_fields = {
            name: {column: kind for column, kind in relation["columns"].items()
                   if kind in {"date", "timestamptz"}}
            for name, relation in self.relations.items()
        }
        # A dated child can refer to a non-root child whose own event date is
        # outside the window (for example, a recent bill-action committee edge
        # can point to an older action record).  Keep those references explicit
        # so dependency closure includes the parent row instead of emitting a
        # false selected-foreign-key gap.
        self.relation_references = {}

    def _temporal_values(self, name: str, row: dict) -> list[datetime | date]:
        values = []
        for column, kind in self.date_fields.get(name, {}).items():
            parsed = parse_temporal(row.get(column), kind)
            if parsed is not None:
                if isinstance(parsed, datetime):
                    values.append(parsed.astimezone(timezone.utc))
                else:
                    values.append(datetime.combine(parsed, datetime.min.time(), timezone.utc))
        return values

    def _qualifies(self, name: str, row: dict) -> bool:
        return any(self.since <= value <= self.as_of for value in self._temporal_values(name, row))

    def _interval_overlaps(self, row: dict, start: str, end: str) -> bool:
        start_value = parse_temporal(row.get(start), self.date_fields.get("", {}).get(start, "date"))
        end_value = parse_temporal(row.get(end), "date")
        start_dt = (datetime.combine(start_value, datetime.min.time(), timezone.utc)
                    if isinstance(start_value, date) and not isinstance(start_value, datetime)
                    else start_value)
        end_dt = (datetime.combine(end_value, datetime.max.time(), timezone.utc)
                  if isinstance(end_value, date) and not isinstance(end_value, datetime)
                  else end_value)
        if start_dt is None:
            return False
        if end_dt is None:
            end_dt = self.as_of
        return start_dt <= self.as_of and end_dt >= self.since

    def _special_row(self, name: str, row: dict) -> bool:
        if name == "committees_members":
            return str(row.get("congress")) == "119"
        if name == "members_terms":
            try:
                return int(row.get("start_year")) >= self.since.year and int(row.get("start_year")) <= self.as_of.year
            except (TypeError, ValueError):
                return False
        if name == "committees_history":
            return self._interval_overlaps(row, "started_at", "ended_at")
        if name == "congresses_sessions":
            return self._interval_overlaps(row, "start_date", "end_date")
        return False

    def discover_roots(self) -> None:
        # Establish every available root key and select roots with explicit
        # qualifying dates.  Child observations are scanned below to establish
        # dependency roots even when the root itself predates the cutoff.
        for family, spec in self.roots.items():
            key = spec["primary_key"][0]
            for row in self.rows(family):
                self.available[family].add(row[key])
                if self._qualifies(family, row):
                    self.primary[family].add(row[key])

        # Congress identity is a semantic date partition rather than merely a
        # timestamped API row; the 119th Congress begins inside this window.
        for row in self.rows("congresses"):
            try:
                if int(row.get("start_year")) <= self.as_of.year and int(row.get("end_year")) >= self.since.year:
                    self.primary["congresses"].add(row["congress_number"])
            except (TypeError, ValueError):
                pass

        relation_families = {
            name: next((f["name"] for f in self.families
                        if any(c["table"] == name for c in f.get("children", []))), None)
            for name in self.relations if name not in self.roots
        }
        # Resolve child-to-child dependencies to a fixed point.  The first
        # pass selects rows by date/root policy; later passes retain parent rows
        # referenced by those selections and propagate any further references.
        self.relation_references = {name: {} for name in self.relations}
        for _ in range(4):
            changed = False
            for name, relation in self.relations.items():
                if name in self.roots:
                    continue
                family = relation_families.get(name)
                refs = self.relation_references[name]
                for row in self.rows(name):
                    selected = self._special_row(name, row) or self._qualifies(name, row)
                    if family and row.get(self.roots[family]["primary_key"][0]) in self.primary[family]:
                        selected = selected or not self.date_fields.get(name)
                    if any(str(row.get(column)) in values for column, values in refs.items()):
                        selected = True
                    if not selected:
                        continue
                    self.references_from(relation, row)
                    if family == "members" and row.get("bioguide_id"):
                        self.primary["members"].add(row["bioguide_id"])
                    if family == "committees" and row.get("committee_code"):
                        self.primary["committees"].add(row["committee_code"])
                    for column, target in relation.get("foreign_keys", {}).items():
                        target_name, target_column = target.split(".")
                        if target_name in self.roots or row.get(column) is None:
                            continue
                        target_refs = self.relation_references[target_name].setdefault(target_column, set())
                        before = len(target_refs)
                        target_refs.add(str(row[column]))
                        changed |= len(target_refs) != before
            if not changed:
                break

    def selected_child(self, family: str, name: str, row: dict) -> bool:
        if name in self.supplemental_names:
            return row.get("bill_id") in self.primary["bills"]
        # Committee assignment rows are partitioned by Congress rather than
        # by an update timestamp. The date-window product carries the current
        # (119th) roster only; do not pull historical rosters merely because
        # the committee itself has a recent bill/report row.
        if name == "committees_members":
            return self._special_row(name, row)
        owner = self.roots[family]["primary_key"][0]
        if self._special_row(name, row) or self._qualifies(name, row):
            return True
        if any(str(row.get(column)) in values
               for column, values in self.relation_references.get(name, {}).items()):
            return True
        if row.get(owner) not in self.primary[family]:
            return False
        # Non-dated children are structural closure for a selected root.
        return not self.date_fields.get(name)

    def run(self):
        result = super().run()
        result.pop("congress", None)
        result["scope"] = {
            "kind": "updated_or_event_dated",
            "inclusive_since": self.since.isoformat(),
            "as_of": None if self.as_of == datetime.max.replace(tzinfo=timezone.utc) else self.as_of.isoformat(),
            "unknown_date_policy": "dependency closure only; never treated as qualifying by itself",
        }
        result["release_ready"] = False
        result["remaining_acceptance"].append("date-window source coverage and unknown-date review")
        (self.output / "manifest.json").write_text(__import__("json").dumps(result, indent=2) + "\n")
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=Path("contracts/bicam_public_schema.yaml"))
    parser.add_argument("--since", default="2025-01-24T00:00:00+00:00")
    parser.add_argument("--as-of")
    parser.add_argument("--include-supplemental", action="store_true")
    args = parser.parse_args()
    since = datetime.fromisoformat(args.since)
    as_of = datetime.fromisoformat(args.as_of) if args.as_of else None
    TemporalCandidate(args.snapshot, args.output, args.contract, since, as_of,
                      args.include_supplemental).run()


if __name__ == "__main__":
    main()
