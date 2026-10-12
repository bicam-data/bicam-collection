"""Recover hearing children from exact retained, pre-snapshot GovInfo versions.

Only the previously verified fourteen restored hearing parents are in scope.
Captures their known package/granule payloads by version UUID, then projects
source-available children without production writes or new network harvesting.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlparse, parse_qsl

import psycopg
import pyarrow.parquet as pq
import yaml

from bicam_collection.config import Settings
from bicam_collection.export_semantics import CHAMBERS
from capture_hearing_export_parents import project_parent
from export_bills_parquet import sha
from project_amendment_export_repairs import entity_id, canonical_row


def assert_no_credentials(obj):
    if isinstance(obj, dict):
        for key, item in obj.items():
            if key.lower() in {"api_key", "apikey", "authorization", "access_token"}:
                raise ValueError("credential-bearing source payload cannot be exported verbatim")
            assert_no_credentials(item)
    elif isinstance(obj, list):
        for item in obj:
            assert_no_credentials(item)
    elif isinstance(obj, str) and obj.startswith(("http://", "https://")):
        if any(k.lower() in {"api_key", "apikey", "access_token"} for k, _ in parse_qsl(urlparse(obj).query)):
            raise ValueError("credential-bearing source URL cannot be exported verbatim")


def member_objects(payload):
    """Visit member arrays at every nesting depth, preserving their source paths."""
    def walk(obj, path):
        if isinstance(obj, dict):
            for key, item in obj.items():
                if key == "members" and isinstance(item, list):
                    for i, member in enumerate(item):
                        if not isinstance(member, dict):
                            raise ValueError("unreviewed member array shape")
                        yield f"{path}.{key}[{i}]", member
                yield from walk(item, f"{path}.{key}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                yield from walk(item, f"{path}[{i}]")
    yield from walk(payload, "$")


def folded(text):
    return " ".join(text.split()).casefold()


def resolve_member(source, members, terms):
    supplied = source.get("bioGuideId") or source.get("bioguideId")
    if supplied:
        if not re.fullmatch(r"[A-Z][0-9]{6}", supplied) or supplied not in members:
            return None, "unknown_source_bioguide"
        eligible = [t for t in terms if t["bioguide_id"] == supplied]
    else:
        eligible = terms
    source_chamber = CHAMBERS.get(str(source.get("chamber", "")).lower())
    if not source_chamber or not source.get("state") or str(source.get("congress", "")) != "119":
        return None, "insufficient_source_member_context"
    eligible = [t for t in eligible if t["congress"] == 119 and t["chamber"] == source_chamber
                and t["state_code"] == source["state"]]
    if supplied:
        return (supplied, "exact_source_bioguide_with_term_context") if eligible else (None, "source_bioguide_term_contradiction")
    # No nicknames, fuzzy spelling or title inference: full name AND parsed
    # state must match the frozen member plus Congress/chamber/state-code term.
    found = set()
    for term in eligible:
        member = members[term["bioguide_id"]]
        expected = f"{member['direct_order_name']} of {term['state_name']}"
        if any(folded(n.get("parsed", "")) == folded(expected) for n in source.get("name", [])):
            found.add(term["bioguide_id"])
    if len(found) == 1:
        return next(iter(found)), "unique_exact_full_name_state_congress_chamber"
    return None, "ambiguous_or_unresolved_member_name"


def resolve_committee(source_id, known):
    normalized = str(source_id or "").strip().lower()
    if normalized in known:
        return normalized, "exact_canonical"
    repaired = normalized.replace("o", "0")
    if repaired in known:
        return repaired, "verified_o_to_zero"
    return None, "unresolved_committee_id"


def capture_and_project(candidate, metadata_evidence, output, contract):
    manifest = json.loads((candidate / "manifest.json").read_text())
    evidence = json.loads(metadata_evidence.read_text())
    if evidence["candidate_manifest_sha256"] != sha(candidate / "manifest.json"):
        raise ValueError("metadata evidence belongs to another baseline candidate")
    entries = [e for group in evidence["additions"].values() for e in group]
    if len(entries) != 28:
        raise ValueError("expected the bounded fourteen package/granule pairs")
    expected = {e["evidence"]["raw_response_version_id"]: e for e in entries}
    if len(expected) != len(entries):
        raise ValueError("duplicate source version")
    families = yaml.safe_load(contract.read_text())["families"]
    family = next(f for f in families if f["name"] == "hearings")
    relations = {r["table"]: r for r in family["children"]}
    inventory = {t["name"]: t for t in manifest["tables"]}
    existing = {}
    for name in ["hearings", "members", "members_terms", "committees", "bills", *relations]:
        entry = inventory[name]
        path = candidate / entry["file"]
        if path.is_symlink() or path.parent.resolve() != candidate.resolve() or sha(path) != entry["sha256"]:
            raise ValueError("baseline candidate changed")
        existing[name] = pq.read_table(path).to_pylist()
    members = {r["bioguide_id"]: r for r in existing["members"]}
    committees = {r["committee_code"] for r in existing["committees"]}
    bills = {r["bill_id"] for r in existing["bills"]}
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    with psycopg.connect(settings.database_url, options=("-c default_transaction_read_only=on "
            "-c statement_timeout=8000 -c lock_timeout=1000 -c max_parallel_workers_per_gather=0")) as connection:
        rows = connection.execute("""
            select id::text, endpoint_url, response_hash, response, first_seen_at
            from raw_response_versions where id = any(%s::uuid[])
        """, (list(expected),)).fetchall()
    if len(rows) != len(expected):
        raise ValueError("required pre-snapshot source version no longer retained")
    payloads = {}
    for version, url, response_hash, payload, first_seen in rows:
        entry = expected[version]
        identity = entry["row"]["hearing_id"]
        if (response_hash != entry["evidence"]["raw_response_hash"] or url != entry["evidence"]["official_url"]
                or first_seen > datetime.fromisoformat(manifest["database_snapshot_at"])):
            raise ValueError("retained evidence does not match the frozen observation")
        actual_hash = hashlib.md5(json.dumps(payload, sort_keys=True).encode(), usedforsecurity=False).hexdigest()
        if actual_hash != response_hash:
            raise ValueError("retained payload differs from its recorded content hash")
        if payload.get("packageId") != entry["row"]["govinfo_package_id"]:
            raise ValueError("package identity contradiction")
        if "govinfo_granule_id" in entry["row"]:
            if payload.get("granuleId") != entry["row"]["govinfo_granule_id"]:
                raise ValueError("granule identity contradiction")
        else:
            project_parent(identity, payload)
        assert_no_credentials(payload)
        encoded = (json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2) + "\n").encode()
        payloads[version] = (identity, entry, payload, encoded)
    output.mkdir(parents=True, exist_ok=False)
    additions = defaultdict(list)
    # Carry earlier metadata additions forward so this overlay remains relative
    # to candidate-1 and can be combined with the unchanged amendment supplement.
    for name, group in evidence["additions"].items():
        additions[name].extend(group)
    resolutions, unresolved, captured = [], [], []
    for version, (identity, entry, payload, encoded) in sorted(payloads.items()):
        filename = f"{version}.json"
        with (output / filename).open("xb") as stream:
            stream.write(encoded)
        provenance = {**entry["evidence"], "payload_file": filename,
                      "payload_sha256": hashlib.sha256(encoded).hexdigest()}
        captured.append({"hearing_id": identity, **provenance})
        def add(name, row, path):
            additions[name].append({"row": row, "evidence": {**provenance, "source_path": path}})
        if "granuleId" not in payload:
            for i, held in enumerate(payload.get("heldDates", [])):
                add("hearings_dates", {"hearing_id": identity, "hearing_date": date.fromisoformat(held).isoformat()}, f"$.heldDates[{i}]")
        for i, committee in enumerate(payload.get("committees", [])):
            code, status = resolve_committee(committee.get("authorityId"), committees)
            if code is None:
                unresolved.append({"hearing_id": identity, "source_path": f"$.committees[{i}]", "status": status, "source": committee})
            else:
                add("hearings_committees", {"hearing_id": identity, "committee_code": code,
                    "committee_name": committee.get("committeeName") or None}, f"$.committees[{i}]")
        witnesses = payload.get("witnesses") or []
        witnesses = [witnesses] if isinstance(witnesses, str) else witnesses
        for i, witness in enumerate(witnesses):
            if not isinstance(witness, str):
                raise ValueError("unreviewed witness shape")
            if witness.strip():
                add("hearings_witnesses", {"hearing_id": identity, "witness": witness.strip()}, f"$.witnesses[{i}]")
        for path, member in member_objects(payload):
            bioguide, status = resolve_member(member, members, existing["members_terms"])
            record = {"hearing_id": identity, "source_path": path, "source": member,
                      "resolved_bioguide_id": bioguide, "status": status, "raw_response_version_id": version}
            resolutions.append(record)
            if bioguide is None:
                unresolved.append(record)
            else:
                add("hearings_members", {"hearing_id": identity, "bioguide_id": bioguide}, path)
        for i, reference in enumerate(payload.get("references", [])):
            if str(reference.get("collectionCode", "")).upper() != "BILLS":
                continue
            for j, content in enumerate(reference.get("contents", [])):
                bill_id = entity_id(content, {"hr", "s", "hres", "sres", "hjres", "sjres", "hconres", "sconres"})
                path = f"$.references[{i}].contents[{j}]"
                if bill_id not in bills:
                    unresolved.append({"hearing_id": identity, "source_path": path, "status": "referenced_bill_not_in_candidate", "bill_id": bill_id})
                else:
                    add("hearings_bills", {"hearing_id": identity, "bill_id": bill_id}, path)
    for name, group in additions.items():
        relation = relations[name]
        known = {tuple(canonical_row(relation["columns"], row)[c] for c in relation["primary_key"]):
                 canonical_row(relation["columns"], row) for row in existing[name]}
        unique = []
        for entry in group:
            row = canonical_row(relation["columns"], entry["row"])
            key = tuple(row[c] for c in relation["primary_key"])
            if key in known:
                if known[key] != row:
                    raise ValueError(f"conflicting existing or proposed child: {name}")
                continue
            known[key] = row
            unique.append({**entry, "row": row})
        additions[name] = unique
    report = {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
        "candidate_manifest_sha256": sha(candidate / "manifest.json"), "source_manifest_sha256": manifest["source_manifest_sha256"],
        "database_snapshot_at": manifest["database_snapshot_at"], "metadata_evidence_sha256": sha(metadata_evidence),
        "contract_sha256": sha(contract), "captured_versions": captured, "additions": dict(additions),
        "member_resolutions": resolutions, "unresolved": unresolved,
        "prior_unresolved_metadata_lookups": evidence["unresolved"], "production_modified": False,
        "scope": "Fourteen restored parents; retained pre-snapshot versions only. Not a population-wide hearing completeness claim."}
    with (output / "report.json").open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("candidate", "metadata-evidence", "output"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=Path("contracts/bicam_public_schema.yaml"))
    args = parser.parse_args()
    report = capture_and_project(args.candidate, args.metadata_evidence, args.output, args.contract)
    print(json.dumps({"additions": {k: len(v) for k, v in report["additions"].items()},
                      "unresolved": len(report["unresolved"])}))
