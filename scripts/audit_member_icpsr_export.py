"""Capture a bounded Voteview member file and audit original ICPSR feature parity.

Exact Bioguide plus Congress/chamber/state context only; no fuzzy member match,
no ideology export and no production writes. Proposals are not applied here.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import csv
import io
import json
from pathlib import Path
import re
import zipfile

import pyarrow.parquet as pq
import requests

from export_bills_parquet import sha
from load_family_parquet import validate

URL = "https://voteview.com/static/data/out/members/HS119_members.csv"


def normalize_icpsr(raw):
    if raw in (None, "", "NA", "NaN", "nan", "None", r"\N"):
        return None
    if not re.fullmatch(r"[0-9]+(?:\.0+)?", raw) or int(raw.split(".")[0]) <= 0:
        raise ValueError("unreviewed ICPSR serialization")
    # Original BICAM stores ICPSR as text with leading zeroes for older IDs.
    return str(int(raw.split(".")[0])).zfill(5)


def propose(members, terms, voteview, published):
    source = defaultdict(list)
    owners = defaultdict(set)
    for row in voteview:
        if row["congress"] != "119":
            raise ValueError("unexpected Congress in Voteview 119 file")
        if row["chamber"] not in {"House", "Senate"} or not row["bioguide_id"]:
            continue
        identifier = normalize_icpsr(row["icpsr"])
        if identifier:
            source[row["bioguide_id"]].append(row)
            owners[identifier].add(row["bioguide_id"])
    result = []
    for member in members:
        bioguide = member["bioguide_id"]
        service = [t for t in terms if t["bioguide_id"] == bioguide and t["congress"] == 119]
        exact = source[bioguide]
        candidates = [r for r in exact if any(r["chamber"].lower() == t["chamber"] and r["state_abbrev"] == t["state_code"] for t in service)]
        identifiers = {normalize_icpsr(r["icpsr"]) for r in candidates}
        legacy = published.get(bioguide)
        entry = {"bioguide_id": bioguide, "name": member["direct_order_name"], "current_icpsr": member["icpsr"],
                 "published_icpsr": legacy, "voteview_rows": exact, "term_context": service}
        if len(identifiers) == 1 and all(len(owners[i]) == 1 for i in identifiers):
            identifier = next(iter(identifiers))
            entry.update({"proposed_icpsr": identifier, "status": "exact_bioguide_congress_chamber_state",
                          "published_comparison": "not_available" if legacy is None else "agrees" if legacy == identifier else "differs_requires_review"})
        elif not exact:
            entry["status"] = "no_Voteview_119_bioguide_record"
        else:
            entry["status"] = "ambiguous_or_conflicting_context"
        result.append(entry)
    return result


def audit(candidate, archive, output):
    manifest = validate(candidate)
    inventory = {t["name"]: t for t in manifest["tables"]}
    members = pq.read_table(candidate / inventory["members"]["file"]).to_pylist()
    terms = pq.read_table(candidate / inventory["members_terms"]["file"]).to_pylist()
    with zipfile.ZipFile(archive) as z:
        names = [n for n in z.namelist() if Path(n).name == "members.csv" and not n.startswith("__MACOSX/")]
        if len(names) != 1 or z.getinfo(names[0]).file_size > 20_000_000:
            raise ValueError("unreviewed published members archive")
        with z.open(names[0]) as stream:
            legacy = list(csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8-sig")))
    if len({r["bioguide_id"] for r in legacy}) != len(legacy):
        raise ValueError("duplicate published Bioguide identities")
    published = {r["bioguide_id"]: normalize_icpsr(r["icpsr"]) for r in legacy}
    with requests.get(URL, stream=True, timeout=(5, 20), allow_redirects=False) as response:
        if response.status_code != 200:
            raise ValueError(f"Voteview download HTTP {response.status_code}")
        raw = bytearray()
        for chunk in response.iter_content(65536):
            raw.extend(chunk)
            if len(raw) > 2_000_000:
                raise ValueError("bounded member CSV size exceeded")
    rows = list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
    if not rows or len(rows) > 1500:
        raise ValueError("unexpected Voteview member row count")
    proposals = propose(members, terms, rows, published)
    output.mkdir(parents=True, exist_ok=False)
    with (output / "HS119_members.csv").open("xb") as stream:
        stream.write(raw)
    report = {"schema_version": 1, "observed_at": datetime.now(timezone.utc).isoformat(),
        "candidate_manifest_sha256": sha(candidate / "manifest.json"), "published_archive_sha256": sha(archive),
        "official_data_url": URL, "source_file": "HS119_members.csv", "source_sha256": sha(output / "HS119_members.csv"),
        "source_rows": len(rows), "candidate_members": len(members), "proposals": proposals,
        "status_counts": dict(Counter(r["status"] for r in proposals)),
        "published_comparison_counts": dict(Counter(r.get("published_comparison", "not_proposed") for r in proposals)),
        "production_modified": False, "applied": False,
        "scope": "Original ICPSR member identifier field only; no roll-call, ideology or new category export.",
        "icpsr_serialization": "Positive integral identifier, text padded to at least five digits as in original BICAM; never a floating-point value.",
        "limits": ["Voteview is a current post-snapshot observation, not retroactive frozen knowledge.",
                   "Missing Voteview rows do not prove a member did not serve; nonvoting members and source coverage need review.",
                   "Historical ICPSR disagreements remain explicit; no blanket copying of prior-Congress values."]}
    with (output / "report.json").open("x") as stream:
        json.dump(report, stream, indent=2, default=str)
        stream.write("\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("candidate", "archive", "output"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    args = parser.parse_args()
    report = audit(args.candidate, args.archive, args.output)
    print(json.dumps({k: report[k] for k in ("source_rows", "candidate_members", "status_counts", "published_comparison_counts")}))
