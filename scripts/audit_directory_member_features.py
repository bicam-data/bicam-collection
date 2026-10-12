"""Offline, evidence-bound inventory of Congress-119 directory member features.

Proposals only: no export mutation, downloads, database calls or fuzzy matching.
Retain source line ranges and distinguish failed parsing from absent membership.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import unicodedata
from urllib.parse import urlsplit

import pyarrow.parquet as pq

from export_bills_parquet import sha
from load_family_parquet import validate

NAME = re.compile(r"^\s*(?P<name>[A-Z][^\n]{1,150}?), (?:Republican|Democrat(?:ic-Farmer-Labor Party)?|Independent)(?: Delegate)?(?:,| of )")
OFFICE = re.compile(r"^\s*Congressional Office\s*$", re.M)
TERM_KEY = ("bioguide_id", "congress", "chamber", "start_year")


def normal_name(value):
    # Case/diacritics/punctuation only; no nickname substitutions or edit distance.
    return " ".join(re.sub(r"[^A-Z0-9]+", " ", unicodedata.normalize("NFKD", value or "")
                           .encode("ascii", "ignore").decode().upper()).split())


def official_url_key(value):
    parts = urlsplit(value or "")
    host = (parts.hostname or "").lower().removeprefix("www.")
    if (parts.scheme not in {"https", "http"} or parts.username or parts.password
            or parts.query or parts.fragment or parts.port not in {None, 80, 443}
            or not host.endswith((".house.gov", ".senate.gov"))):
        return None
    return host + parts.path.rstrip("/")


def source_sections(text, state_names):
    lines = text.splitlines()
    markers = [i for i, line in enumerate(lines) if line.strip() == "<DOC>"]
    sections = []
    for position, start in enumerate(markers):
        end = markers[position + 1] if position + 1 < len(markers) else len(lines)
        title = next((line.strip() for line in lines[start + 1:min(start + 6, end)]
                      if line.strip() not in {"", "."}), "")
        if title not in state_names:
            continue
        block = lines[start:end]
        # The final member section is followed by rosters and institutional prose.
        roster = next((i for i, line in enumerate(block) if line.strip() == "THE SENATE"), len(block))
        end = start + roster
        populations = []
        for i in range(start, end - 1):
            if re.search(r"2020 (?:Census )?Population", lines[i]):
                value = re.match(r"\s*([0-9]{1,3}(?:,[0-9]{3})+)\s+", lines[i + 1])
                if value:
                    populations.append({"value": int(value[1].replace(",", "")), "line_start": i + 1, "line_end": i + 2})
        population = populations[0] if len(populations) == 1 else None
        sections.append({"state_name": state_names[title], "heading": title,
                         "start": start, "end": end, "population": population})
    return lines, sections


def extract(text, state_names):
    lines, sections = source_sections(text, state_names)
    records, failures = [], []
    for section in sections:
        chamber, previous_office = None, section["start"]
        for i in range(section["start"], section["end"]):
            heading = lines[i].strip()
            if heading == "SENATORS":
                chamber = "senate"
            elif heading in {"REPRESENTATIVE", "REPRESENTATIVES", "DELEGATE", "RESIDENT COMMISSIONER"}:
                chamber = "house"
            if not OFFICE.fullmatch(lines[i]):
                continue
            starts = [(j, NAME.match(lines[j])) for j in range(previous_office, i) if NAME.match(lines[j])]
            previous_office = i + 1
            if len(starts) != 1 or chamber is None:
                failures.append({"office_line": i + 1, "state_name": section["state_name"],
                                 "reason": "biography_boundary_or_chamber_unresolved", "start_count": len(starts)})
                continue
            start, match = starts[0]
            biography = "\n".join(line.rstrip() for line in lines[start:i]).strip()
            urls = [(j, lines[j].strip()) for j in range(i + 1, min(i + 6, section["end"]))
                    if lines[j].strip().startswith(("https://", "http://"))]
            url = urls[0][1] if len(urls) == 1 else None
            record = {"source_name": match["name"].strip(), "state_name": section["state_name"],
                      "chamber": chamber, "biography": biography, "line_start": start + 1, "line_end": i,
                      "office_url": url, "url_line": urls[0][0] + 1 if len(urls) == 1 else None,
                      "population": section["population"], "prose_issues": []}
            if "\ufffd" in biography:
                record["prose_issues"].append("source_replacement_character")
            if len(biography) < 100 or "<DOC>" in biography or not re.search(r"election\s+history:", biography):
                record["prose_issues"].append("biography_shape_requires_review")
            records.append(record)
    return records, failures, sections


def match_records(records, members, terms):
    by_id = {m["bioguide_id"]: m for m in members}
    if len(by_id) != len(members):
        raise ValueError("duplicate member identity")
    scoped_terms = [t for t in terms if t["congress"] == 119]
    if len({tuple(t[k] for k in TERM_KEY) for t in scoped_terms}) != len(scoped_terms):
        raise ValueError("duplicate term identity")
    urls = defaultdict(set)
    for member in members:
        key = official_url_key(member["official_url"])
        if key:
            urls[key].add(member["bioguide_id"])
    matches = []
    for record in records:
        identity_name = re.sub(r", M\.D\.$", "", record["source_name"])
        name = normal_name(identity_name)
        name_damaged = "\ufffd" in record["source_name"]
        exact = []
        for term in scoped_terms:
            member = by_id[term["bioguide_id"]]
            aliases = {normal_name(member.get(k)) for k in ("direct_order_name", "normalized_name")}
            if (not name_damaged and name and name in aliases and record["state_name"].casefold() == term["state_name"].casefold()
                    and record["chamber"] == term["chamber"]):
                exact.append(term)
        key = official_url_key(record["office_url"])
        owners = urls[key] if key else set()
        result = {**record, "exact_context_term_keys": [{k: t[k] for k in TERM_KEY} for t in exact],
                  "url_member_ids": sorted(owners), "proposed_fields": {},
                  "identity_name_rule": "remove_explicit_MD_credential" if identity_name != record["source_name"] else "case_diacritics_punctuation_only"}
        # URL agreement is independent identity corroboration, not a fuzzy-name fallback.
        existing_url = official_url_key(by_id[exact[0]["bioguide_id"]]["official_url"]) if len(exact) == 1 else None
        # A directory URL is source evidence even when the current member row
        # has no URL. Reject only a conflicting existing URL or a shared URL.
        if len(exact) == 1 and (not owners or owners == {exact[0]["bioguide_id"]}):
            term = exact[0]
            result.update({"status": "exact_name_state_chamber_and_directory_url", "term_key": {k: term[k] for k in TERM_KEY},
                           "current_url_comparison": "not_available" if existing_url is None else
                           "agrees" if existing_url == key else "differs_source_url_changed"})
            fields = {"other_url": record["office_url"]}
            if not record["prose_issues"]:
                fields["biography"] = record["biography"]
            if record["population"]:
                fields["population"] = record["population"]["value"]
            # Title is a granule-title feature in the published dataset, not the
            # directory's uppercase name or a member's occupational title.
            for field, value in fields.items():
                if term[field] is None:
                    result["proposed_fields"][field] = value
                elif term[field] != value:
                    result.setdefault("existing_value_conflicts", {})[field] = term[field]
        else:
            result["status"] = "identity_requires_review"
        matches.append(result)
    counts = Counter(tuple(r["term_key"][k] for k in TERM_KEY) for r in matches if "term_key" in r)
    for result in matches:
        if "term_key" in result and counts[tuple(result["term_key"][k] for k in TERM_KEY)] > 1:
            result["status"] = "duplicate_source_member_requires_review"
            result["proposed_fields"] = {}
    return matches


def audit(bundle, evidence, output=None):
    manifest = validate(bundle)
    capture_path = evidence / "report.json"
    capture = json.loads(capture_path.read_text())
    source = evidence / capture["file"]
    if source.is_symlink() or source.parent.resolve() != evidence.resolve() or sha(source) != capture["sha256"]:
        raise ValueError("directory evidence hash/path changed")
    text = source.read_bytes().decode("utf-8")
    cutoff = re.search(r"Closing date for compilation of the Congressional Directory was ([^.]+)\.", text)
    if not cutoff or cutoff[1] != "October 1, 2025" or "119th Congress" not in text:
        raise ValueError("unreviewed directory edition or compilation cutoff")
    inventory = {t["name"]: t for t in manifest["tables"]}
    members = pq.read_table(bundle / inventory["members"]["file"]).to_pylist()
    terms = pq.read_table(bundle / inventory["members_terms"]["file"]).to_pylist()
    states = {t["state_name"].upper(): t["state_name"] for t in terms if t["congress"] == 119}
    states["THE VIRGIN ISLANDS OF THE UNITED STATES"] = "Virgin Islands"
    records, failures, sections = extract(text, states)
    matches = match_records(records, members, terms)
    matched = {r["term_key"]["bioguide_id"] for r in matches if r["status"] == "exact_name_state_chamber_and_directory_url"}
    report = {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
              "candidate_manifest_sha256": sha(bundle / "manifest.json"), "capture_report_sha256": sha(capture_path),
              "source_sha256": capture["sha256"], "source_observed_at": capture["observed_at"],
              "source_url": capture["official_text_url"], "compilation_cutoff": "2025-10-01",
              "publication_date": "2026-02-20", "sections": sections,
              "candidate_members": len(members), "parsed_source_members": len(records),
              "unparsed_offices": failures, "status_counts": dict(Counter(r["status"] for r in matches)),
              "proposed_field_counts": dict(Counter(k for r in matches for k in r["proposed_fields"])),
              "prose_issue_counts": dict(Counter(k for r in matches for k in r["prose_issues"])),
              "source_replacement_characters": text.count("\ufffd"),
              "social_platform_token_counts": {k: len(re.findall(p, text, flags=re.I)) for k, p in {
                  "twitter": r"\btwitter\b|twitter\.com", "instagram": r"\binstagram\b|instagram\.com",
                  "facebook": r"\bfacebook\b|facebook\.com", "youtube": r"\byoutube\b|youtube\.com",
                  "x_domain": r"https?://(?:www\.)?x\.com/"}.items()},
              "unmatched_candidate_members": [{"bioguide_id": m["bioguide_id"], "name": m["direct_order_name"]}
                                              for m in members if m["bioguide_id"] not in matched],
              "records": matches, "applied_to_export": False, "production_modified": False,
              "limits": ["Audit proposals only; review boundaries/fidelity and original semantics before projection.",
                         "Unmatched does not prove absence: inspect parsing, aliases, URL changes and source cohort.",
                         "State-level 2020 Census population, not district population or a current-year estimate.",
                         "Title remains pending granule-title parity; social URL absence is scoped to this captured text.",
                         "Damaged biographies are withheld, never silently repaired or replaced with older-Congress prose."]}
    if output is not None:
        output.mkdir(parents=True, exist_ok=False)
        with (output / "report.json").open("x") as stream:
            json.dump(report, stream, indent=2, default=str)
            stream.write("\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("bundle", "evidence", "output"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.bundle, args.evidence, args.output)
    print(json.dumps({k: result[k] for k in ("candidate_members", "parsed_source_members", "status_counts",
                                             "proposed_field_counts", "prose_issue_counts", "social_platform_token_counts")}))
