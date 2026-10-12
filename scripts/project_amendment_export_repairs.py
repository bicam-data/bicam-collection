"""Project captured official amendment metadata into an additive export overlay.

Reads frozen Parquet and verified local evidence. A read-only PostgreSQL
connection computes the existing JSONB action identity; no tables are changed.
Does not crawl, fetch document bodies, or load pipeline credentials.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re

import psycopg
from psycopg.types.json import Jsonb
import pyarrow.parquet as pq
import yaml

from bicam_collection.export_semantics import normalize_value
from capture_missing_amendment_details import verify_identity
from export_bills_parquet import sha, value


def checked_json(folder, filename, digest):
    path = folder / filename
    if path.is_symlink() or path.parent.resolve() != folder.resolve() or sha(path) != digest:
        raise ValueError("evidence path/hash mismatch")
    return json.loads(path.read_text())


def canonical_row(columns, row):
    result = {}
    for column, kind in columns.items():
        raw = None if column == "raw_text" else row.get(column)
        if raw is not None and not isinstance(raw, str):
            raw = str(raw).lower() if isinstance(raw, bool) else str(raw)
        normalized, reason = normalize_value("amendments", column, raw)
        if reason:
            raise ValueError(f"unreviewed semantic value: {column}")
        result[column] = value(normalized, kind)
    return json.loads(json.dumps(result, default=str))


def entity_id(obj, types):
    kind = str(obj.get("type", "")).lower()
    number = str(obj.get("number", "")).replace("½", ".5")
    congress = str(obj.get("congress", ""))
    if kind not in types or not re.fullmatch(r"[0-9]+(?:\.5)?", number) or not congress.isdigit():
        raise ValueError("invalid referenced entity identity")
    return f"{kind}{number}-{congress}"


def project_root(identity, amendment, previous):
    verify_identity(identity, amendment)
    if previous["congress"] != 119 or previous["chamber"] is not None or previous["updated_at"] is not None:
        raise ValueError("only verified incomplete Congress-119 placeholders may be repaired")
    row = dict(previous)
    mapping = {"chamber": "chamber", "purpose": "purpose", "description": "description",
               "proposed_at": "proposedDate", "submitted_at": "submittedDate", "updated_at": "updateDate"}
    for field, source in mapping.items():
        incoming = amendment.get(source) or None
        if incoming is not None:
            row[field] = incoming
    if not row["chamber"] or not row["updated_at"]:
        raise ValueError("captured detail does not repair placeholder metadata")
    for field, source, number in (("is_bill_amendment", "amendedBill", "number"),
                                   ("is_treaty_amendment", "amendedTreaty", "treatyNumber"),
                                   ("is_amendment_amendment", "amendedAmendment", "number")):
        row[field] = bool((amendment.get(source) or {}).get(number))
    notes = {n["text"] for n in amendment.get("notes", []) if n.get("text", "").strip()}
    if notes:
        row["notes"] = "\n".join(sorted(notes))
    row["actions_count"] = int((amendment.get("actions") or {}).get("count", 0))
    row["cosponsors_count"] = int((amendment.get("cosponsors") or {}).get("count", 0))
    # Existing public field counts outgoing amended-parent edges, not children.
    row["amendments_to_amendment_count"] = int(row["is_amendment_amendment"])
    return row


def project_text(identity, text):
    formats = text.get("formats") or []
    result = {"amendment_id": identity, "date": date_only(text.get("date")),
              "type": text.get("type") or None, "raw_text": None}
    for kind in ("PDF", "HTML"):
        urls = [f["url"] for f in formats if f.get("type") == kind and f.get("url")]
        result[kind.lower()] = max(urls) if urls else None
    return result


def date_only(raw):
    if not raw:
        return None
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.date().isoformat()


def source_count_review(amendment, task, items):
    if task["advertised_count_matches"]:
        return None
    detail = amendment.get(task["resource"]) or {}
    withdrawn = sum(bool(i.get("sponsorshipWithdrawnDate")) for i in items)
    explained = (task["resource"] == "cosponsors"
                 and detail.get("countIncludingWithdrawnCosponsors") == len(items)
                 and len(items) - withdrawn == detail.get("count"))
    return {"amendment_id": task["amendment_id"], "resource": task["resource"],
            "advertised_count": task["advertised_count"], "returned_count": len(items),
            "withdrawn_count": withdrawn,
            "status": "explained_by_withdrawn_cosponsors" if explained else "unresolved_source_count_difference"}


def project(candidate, details, children, contract, connection):
    manifest = json.loads((candidate / "manifest.json").read_text())
    if manifest["congress"] != 119:
        raise ValueError("this bounded repair is only for Congress 119")
    family = next(f for f in yaml.safe_load(contract.read_text())["families"] if f["name"] == "amendments")
    relations = {r["table"]: r for r in [family, *family["children"]]}
    inventory = {r["name"]: r for r in manifest["tables"]}
    existing = {}
    for name in relations:
        entry = inventory[name]
        path = candidate / entry["file"]
        if path.is_symlink() or path.parent.resolve() != candidate.resolve() or sha(path) != entry["sha256"]:
            raise ValueError("candidate relation changed")
        existing[name] = [canonical_row(relations[name]["columns"], r) for r in pq.read_table(path).to_pylist()]
    roots = {r["amendment_id"]: r for r in existing["amendments"]}
    detail_report = json.loads((details / "report.json").read_text())
    child_report = json.loads((children / "report.json").read_text())
    if detail_report["candidate_manifest_sha256"] != sha(candidate / "manifest.json") or detail_report["unattempted"]:
        raise ValueError("detail capture is incomplete or bound to a different candidate")
    if child_report["detail_report_sha256"] != sha(details / "report.json"):
        raise ValueError("child report is bound to different details")
    if child_report["unattempted"] or any(r["status"] != "complete" for r in child_report["results"]):
        raise ValueError("child capture incomplete")
    if not 0 < len(detail_report["results"]) <= 250:
        raise ValueError("bounded amendment scope exceeded")
    amendments, replacements, pending, exceptions = {}, [], defaultdict(list), []
    for entry in detail_report["results"]:
        if entry["status"] != "identity_verified":
            raise ValueError("unverified detail")
        identity = entry["amendment_id"]
        if identity in amendments:
            raise ValueError("duplicate detail identity")
        amendment = checked_json(details, entry["file"], entry["sanitized_payload_sha256"])["amendment"]
        amendments[identity] = amendment
        projected = canonical_row(family["columns"], project_root(identity, amendment, roots[identity]))
        replacements.append({"expected": roots[identity], "row": projected, "detail_evidence": entry})
        for source, target, kinds in (("amendedBill", "bill_id", {"hr", "s", "hres", "sres", "hjres", "sjres", "hconres", "sconres"}),
                                      ("amendedAmendment", "amended_amendment_id", {"hamdt", "samdt", "suamdt"})):
            obj = amendment.get(source) or {}
            if obj.get("number"):
                relation = "amendments_amended_bills" if source == "amendedBill" else "amendments_amended_amendments"
                pending[relation].append({"amendment_id": identity, target: entity_id(obj, kinds)})
        treaty = amendment.get("amendedTreaty") or {}
        if treaty.get("treatyNumber"):
            if not str(treaty.get("congress", "")).isdigit() or not str(treaty["treatyNumber"]).isdigit():
                raise ValueError("unreviewed treaty identity")
            pending["amendments_amended_treaties"].append({"amendment_id": identity,
                "treaty_id": f"td{treaty['congress']}-{treaty['treatyNumber']}"})
        for sponsor in amendment.get("sponsors", []):
            if sponsor.get("bioguideId"):
                pending["amendments_sponsors"].append({"amendment_id": identity, "bioguide_id": sponsor["bioguideId"]})
            else:
                exceptions.append({"amendment_id": identity, "status": "non_member_sponsor_preserved_in_evidence",
                                   "sponsor": sponsor})
    action_inputs, votes = [], []
    seen_tasks = set()
    for task in child_report["results"]:
        identity, resource = task["amendment_id"], task["resource"]
        if (identity, resource) in seen_tasks:
            raise ValueError("duplicate captured child task")
        seen_tasks.add((identity, resource))
        items = []
        for page in task["pages"]:
            payload = checked_json(children, page["file"], page["sanitized_payload_sha256"])
            items.extend(payload[task["payload_key"]])
        if len(items) != task["items"]:
            raise ValueError("child item count differs from report")
        review = source_count_review(amendments[identity], task, items)
        if review:
            exceptions.append(review)
        if resource == "actions":
            action_inputs.extend({"amendment_id": identity, "action": item} for item in items)
        elif resource == "textVersions":
            pending["amendments_texts"].extend(project_text(identity, item) for item in items)
        elif resource == "cosponsors":
            for item in items:
                if not re.fullmatch(r"[A-Z][0-9]{6}", item.get("bioguideId", "")):
                    raise ValueError("missing or invalid cosponsor Bioguide ID")
                pending["amendments_cosponsors"].append({"amendment_id": identity, "bioguide_id": item["bioguideId"]})
        elif resource == "amendmentsToAmendment":
            for item in items:
                child_id = entity_id(item, {"hamdt", "samdt", "suamdt"})
                if child_id in amendments:
                    parent = amendments[child_id].get("amendedAmendment") or {}
                    if entity_id(parent, {"hamdt", "samdt", "suamdt"}) != identity:
                        raise ValueError("contradictory reciprocal amendment identity")
                pending["amendments_amended_amendments"].append({"amendment_id": child_id, "amended_amendment_id": identity})
        else:
            raise ValueError("unreviewed child resource")
    # PostgreSQL's canonical JSONB serialization is part of the stable action ID.
    # No table is read or written, and the caller enforces read-only mode.
    hashed = connection.execute("""
        select item->>'amendment_id', item->'action',
               md5((item->>'amendment_id') || '|' || (item->'action')::text)::uuid::text
        from jsonb_array_elements(%s::jsonb) with ordinality as t(item, ordinal)
        order by ordinal
    """, (Jsonb(action_inputs),)).fetchall()
    for identity, action, action_id in hashed:
        system = action.get("sourceSystem") or {}
        pending["amendments_actions"].append({"amendment_id": identity, "action_id": action_id,
            "action_code": action.get("actionCode") or None, "action_date": date_only(action.get("actionDate")),
            "text": action.get("text") or None, "action_type": action.get("type") or None,
            "source_system": system.get("name") or None,
            "source_system_code": str(int(system["code"])) if system.get("code") not in (None, "") else None})
        for vote in action.get("recordedVotes", []):
            votes.append({"amendment_id": identity, "action_id": action_id, "chamber": vote.get("chamber"),
                "congress": vote.get("congress"), "date": date_only(vote.get("date")),
                "roll_number": vote.get("rollNumber"), "session": vote.get("sessionNumber"), "url": vote.get("url")})
    # Match the public latest-date/url ordering at the declared vote grain.
    vote_keys = {}
    vote_relation = relations["amendments_actions_recorded_votes"]
    for row in sorted(votes, key=lambda r: (r["date"] or "", r["url"] or "")):
        row = canonical_row(vote_relation["columns"], row)
        key = tuple(row[c] for c in vote_relation["primary_key"])
        if any(v is None for v in key):
            raise ValueError("incomplete captured vote identity requires review")
        vote_keys[key] = row
    pending["amendments_actions_recorded_votes"] = list(vote_keys.values())
    additions, duplicates = {}, Counter()
    for name, rows in pending.items():
        relation = relations[name]
        keys = relation.get("primary_key") or list(relation["columns"])
        seen = {tuple(row[c] for c in keys): row for row in existing[name]}
        additions[name] = []
        for raw in rows:
            row = canonical_row(relation["columns"], raw)
            key = tuple(row[c] for c in keys)
            if key in seen:
                if seen[key] != row:
                    raise ValueError(f"conflicting existing or supplemental child: {name}")
                duplicates[name] += 1
                continue
            seen[key] = row
            additions[name].append(row)
    return {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
        "source_manifest_sha256": manifest["source_manifest_sha256"], "candidate_manifest_sha256": sha(candidate / "manifest.json"),
        "contract_sha256": sha(contract), "detail_report_sha256": sha(details / "report.json"),
        "child_report_sha256": sha(children / "report.json"),
        "temporal_scope": "Frozen base plus explicitly supplemental official observations captured after the base snapshot.",
        "root_replacements": replacements, "child_additions": additions, "duplicate_edges_or_renditions_skipped": dict(duplicates),
        "exceptions": exceptions, "production_modified": False, "applied": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("candidate", "details", "children", "output"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=Path("contracts/bicam_public_schema.yaml"))
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve earlier evidence")
    dsn = os.environ.get("BICAM_EXPORT_TEST_DSN")
    if not dsn:
        parser.error("BICAM_EXPORT_TEST_DSN is required for read-only PostgreSQL JSONB identity computation")
    with psycopg.connect(dsn, options="-c default_transaction_read_only=on -c statement_timeout=10000 -c timezone=UTC") as connection:
        result = project(args.candidate, args.details, args.children, args.contract, connection)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"root_replacements": len(result["root_replacements"]),
                      "child_additions": {k: len(v) for k, v in result["child_additions"].items()},
                      "exception_statuses": dict(Counter(e["status"] for e in result["exceptions"]))}))
