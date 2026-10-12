"""Bounded official GETs for incomplete Congress-119 amendment export records.

Writes a new local evidence bundle only; no production database or crawler-state
writes. Never persists credentials or request URLs containing API keys.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time

import pyarrow.parquet as pq
import requests

from bicam_collection.config import Settings


def sanitize(obj, secret):
    if isinstance(obj, dict):
        return {k: sanitize(v, secret) for k, v in obj.items()
                if k.lower() not in {"api_key", "apikey", "authorization", "access_token"}}
    if isinstance(obj, list):
        return [sanitize(v, secret) for v in obj]
    if isinstance(obj, str):
        return obj.replace(secret, "[REDACTED]") if secret else obj
    return obj


def verify_identity(expected, amendment):
    m = re.fullmatch(r"(hamdt|samdt|suamdt)([0-9]+)-119", expected)
    if not m or str(amendment.get("congress")) != "119" or str(amendment.get("number")) != m[2] or str(amendment.get("type", "")).lower() != m[1]:
        raise ValueError("official response identity contradicts requested amendment")


def capture(candidate, output, interval=1.0):
    manifest_path = candidate / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    record = next(t for t in manifest["tables"] if t["name"] == "amendments")
    path = candidate / record["file"]
    with path.open("rb") as f:
        if hashlib.file_digest(f, "sha256").hexdigest() != record["sha256"]:
            raise ValueError("candidate amendment artifact changed")
    targets = [r["amendment_id"] for r in pq.read_table(path, columns=["amendment_id", "congress", "updated_at", "chamber"]).to_pylist()
               if r["congress"] == 119 and r["updated_at"] is None and r["chamber"] is None]
    if len(targets) > 250:
        raise ValueError("bounded repair scope exceeded")
    Settings.from_env(Path(__file__).resolve().parents[1])
    values = [os.environ.get(k, "") for k in ("CONGRESS_API_KEYS", "CONGRESSIONAL_API_KEY")]
    values += [v for k, v in sorted(os.environ.items()) if k.startswith("CONGRESS_API_KEY_")]
    key = next((v.split(",")[0].strip() for v in values if v.strip()), None)
    if key is None:
        raise ValueError("Congress credentials unavailable")
    output.mkdir(parents=True, exist_ok=False)
    started = datetime.now(timezone.utc).isoformat()
    results, last_start = [], 0.0
    with requests.Session() as session, (output / "progress.jsonl").open("x") as progress:
        for ordinal, identity in enumerate(targets, 1):
            m = re.fullmatch(r"(hamdt|samdt|suamdt)([0-9]+)-119", identity)
            if not m:
                raise ValueError("unreviewed amendment identifier")
            url = f"https://api.congress.gov/v3/amendment/119/{m[1]}/{m[2]}"
            time.sleep(max(0.0, interval - (time.monotonic() - last_start)))
            last_start = time.monotonic()
            result = {"amendment_id": identity, "official_url": url,
                      "observed_at": datetime.now(timezone.utc).isoformat()}
            try:
                with session.get(url, params={"api_key": key, "format": "json"}, timeout=(5, 20), stream=True) as response:
                    result["http_status"] = response.status_code
                    data = bytearray()
                    for chunk in response.iter_content(65536):
                        data.extend(chunk)
                        if len(data) > 2_000_000:
                            raise ValueError("bounded response limit exceeded")
                    result["response_sha256"] = hashlib.sha256(data).hexdigest()
                    if response.status_code == 200:
                        amendment = json.loads(data).get("amendment", {})
                        verify_identity(identity, amendment)
                        payload = {"amendment": sanitize(amendment, key)}
                        filename = identity + ".json"
                        encoded = (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode()
                        (output / filename).write_bytes(encoded)
                        result.update({"status": "identity_verified", "file": filename,
                                       "sanitized_payload_sha256": hashlib.sha256(encoded).hexdigest(),
                                       "source_updated_at": amendment.get("updateDate"),
                                       "child_resources": {k: sanitize(v, key) for k, v in amendment.items()
                                                           if isinstance(v, dict) and ("url" in v or "count" in v)}})
                    else:
                        result["status"] = "http_error"
            except (requests.RequestException, ValueError) as error:
                # Exception text can contain credential-bearing URLs; keep only class.
                result.update({"status": "request_or_validation_error", "error_class": type(error).__name__})
            results.append(result)
            progress.write(json.dumps(result) + "\n")
            progress.flush()
            if ordinal % 20 == 0 or ordinal == len(targets):
                print(json.dumps({"completed": ordinal, "total": len(targets),
                                  "verified": sum(r["status"] == "identity_verified" for r in results)}), flush=True)
            if result.get("http_status") == 429:
                print("Rate pressure encountered; stopping bounded run without retries.", flush=True)
                break
    report = {"schema_version": 1, "candidate_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
              "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat(),
              "scope": "incomplete Congress-119 amendment details", "target_count": len(targets),
              "request_count": len(results), "unattempted": targets[len(results):], "results": results,
              "production_modified": False, "applied_to_export": False,
              "temporal_scope": "Fresh official observations after the base snapshot, explicitly supplemental, not backdated to the snapshot."}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    capture(args.candidate, args.output)
