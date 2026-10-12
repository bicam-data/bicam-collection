"""Bounded official checks for four remaining hearing metadata gaps.

Five GovInfo summary GETs (including the known alternate House package), one
bounded transcript opening, and one read-only local Congress source query.
No source-absence inference from search misses or transient errors.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time

import psycopg
import requests

from bicam_collection.config import Settings
from capture_missing_amendment_details import sanitize
from project_hearing_export_children import assert_no_credentials

PACKAGES = ("CHRG-119hhrg59805", "CHRG-119jhrg58326", "CHRG-119shrg60081", "CHRG-119shrg61162", "CHRG-119hhrg60081")
TRANSCRIPT = "https://www.govinfo.gov/content/pkg/CHRG-119hhrg60081/html/CHRG-119hhrg60081.htm"


def audit(output):
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    values = [os.environ.get(k, "") for k in ("GOVINFO_API_KEYS", "GOVINFO_API_KEY")]
    values += [v for k, v in sorted(os.environ.items()) if k.startswith("GOVINFO_API_KEY_")]
    key = next((v.split(",")[0].strip() for v in values if v.strip()), None)
    if not key:
        raise ValueError("GovInfo credentials unavailable")
    output.mkdir(parents=True, exist_ok=False)
    results = []
    with requests.Session() as session:
        for package in PACKAGES:
            url = f"https://api.govinfo.gov/packages/{package}/summary"
            entry = {"package_id": package, "official_url": url, "observed_at": datetime.now(timezone.utc).isoformat()}
            try:
                with session.get(url, params={"api_key": key}, stream=True, timeout=(5, 20), allow_redirects=False) as response:
                    raw = bytearray()
                    for chunk in response.iter_content(65536):
                        raw.extend(chunk)
                        if len(raw) > 1_000_000:
                            raise ValueError("summary exceeds bounded size")
                    entry.update({"http_status": response.status_code, "response_sha256": hashlib.sha256(raw).hexdigest()})
                    payload = sanitize(json.loads(raw), key)
                    if response.status_code == 200 and payload.get("packageId") != package:
                        raise ValueError("official response package identity differs")
                    encoded = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode()
                    filename = f"{package}.json"
                    with (output / filename).open("xb") as stream:
                        stream.write(encoded)
                    entry.update({"file": filename, "sanitized_payload_sha256": hashlib.sha256(encoded).hexdigest(),
                                  "status": "identity_verified" if response.status_code == 200 else "http_response_not_package_presence"})
            except (requests.RequestException, ValueError) as error:
                entry.update({"status": "request_or_validation_error", "error_class": type(error).__name__})
            results.append(entry)
            if entry.get("http_status") == 429:
                break
            time.sleep(1)
        opening = {"official_url": TRANSCRIPT, "observed_at": datetime.now(timezone.utc).isoformat()}
        try:
            with session.get(TRANSCRIPT, stream=True, timeout=(5, 20), allow_redirects=False) as response:
                opening["http_status"] = response.status_code
                if response.status_code == 200:
                    raw = response.raw.read(24000, decode_content=True)
                    filename = "CHRG-119hhrg60081-opening.html"
                    with (output / filename).open("xb") as stream:
                        stream.write(raw)
                    opening.update({"file": filename, "prefix_bytes": len(raw), "prefix_sha256": hashlib.sha256(raw).hexdigest(),
                                    "complete_document": False, "status": "bounded_opening_captured"})
        except requests.RequestException as error:
            opening.update({"status": "request_error", "error_class": type(error).__name__})
    with psycopg.connect(settings.database_url, options=("-c default_transaction_read_only=on "
            "-c statement_timeout=8000 -c lock_timeout=1000 -c max_parallel_workers_per_gather=0")) as connection:
        rows = connection.execute("""
            select endpoint_url, response_hash, response, fetched_at
            from raw_responses
            where source = 'congress' and category = 'hearings' and node_type = 'hearing_detail'
              and response #>> '{hearing,congress}' = '119'
              and ltrim(response #>> '{hearing,jacketNumber}', '0') = any(%s)
            limit 30
        """, (["59805", "58326", "60081", "61162"],)).fetchall()
    if len(rows) >= 30:
        raise ValueError("bounded Congress observation inventory exceeded")
    local = []
    for i, (url, response_hash, payload, fetched_at) in enumerate(rows):
        assert_no_credentials(payload)
        encoded = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode()
        filename = f"congress-observation-{i:02d}.json"
        with (output / filename).open("xb") as stream:
            stream.write(encoded)
        local.append({"official_url": url, "raw_response_hash": response_hash, "fetched_at": fetched_at.isoformat(),
                      "file": filename, "payload_sha256": hashlib.sha256(encoded).hexdigest()})
    result = {"schema_version": 1, "observed_at": datetime.now(timezone.utc).isoformat(),
        "govinfo_results": results, "transcript_opening": opening, "local_congress_observations": local,
        "production_modified": False, "applied_to_export": False,
        "limits": ["Current official endpoint observations, not a historical or whole-catalog absence proof.",
                   "Current local Congress observations are not silently substituted for frozen inputs.",
                   "Transcript capture is only an opening prefix; no full-text export.",
                   "House/Senate 60081 observations require a joint-hearing identity decision before release."]}
    with (output / "report.json").open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.output)
    print(json.dumps({"govinfo": [{k: r.get(k) for k in ("package_id", "http_status", "status")} for r in result["govinfo_results"]],
                      "congress_observations": len(result["local_congress_observations"]),
                      "transcript_opening_status": result["transcript_opening"].get("status")}))
