"""Read the explicitly advertised children of the 180 captured amendments.

Bounded official GETs into a new local bundle. Does not fetch linked document
bodies, unrelated bill/member endpoints, or write production ingestion state.
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
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests

from bicam_collection.config import Settings
from capture_missing_amendment_details import sanitize

RESOURCES = {"actions": "actions", "cosponsors": "cosponsors",
             "amendmentsToAmendment": "amendments", "textVersions": "textVersions"}


def safe_child_url(url, identity):
    m = re.fullmatch(r"(hamdt|samdt|suamdt)([0-9]+)-119", identity)
    parsed = urlparse(url)
    prefix = f"/v3/amendment/119/{m[1]}/{m[2]}/" if m else None
    if (not prefix or parsed.scheme != "https" or parsed.netloc != "api.congress.gov"
            or not parsed.path.startswith(prefix)
            or parsed.path[len(prefix):] not in {"actions", "cosponsors", "amendments", "text"}):
        raise ValueError("child or pagination URL escapes the declared amendment scope")
    query = [(k, v) for k, v in parse_qsl(parsed.query) if k.lower() not in {"api_key", "apikey"}]
    return urlunparse(parsed._replace(query=urlencode(query), fragment=""))


def capture(details, output):
    report_path = details / "report.json"
    detail_report = json.loads(report_path.read_text())
    tasks = []
    for entry in detail_report["results"]:
        if entry["status"] != "identity_verified":
            raise ValueError("detail scope not fully verified")
        raw = (details / entry["file"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry["sanitized_payload_sha256"]:
            raise ValueError("captured detail file changed")
        amendment = json.loads(raw)["amendment"]
        for field, payload_key in RESOURCES.items():
            child = amendment.get(field)
            if isinstance(child, dict) and child.get("url"):
                tasks.append({"amendment_id": entry["amendment_id"], "resource": field,
                              "payload_key": payload_key, "url": safe_child_url(child["url"], entry["amendment_id"]),
                              "advertised_count": child.get("count")})
    if len(tasks) > 450:
        raise ValueError("bounded child inventory exceeded")
    Settings.from_env(Path(__file__).resolve().parents[1])
    values = [os.environ.get(k, "") for k in ("CONGRESS_API_KEYS", "CONGRESSIONAL_API_KEY")]
    values += [v for k, v in sorted(os.environ.items()) if k.startswith("CONGRESS_API_KEY_")]
    key = next((v.split(",")[0].strip() for v in values if v.strip()), None)
    if not key:
        raise ValueError("Congress credentials unavailable")
    output.mkdir(parents=True, exist_ok=False)
    completed, request_count, last_start, stop = [], 0, 0.0, False
    with requests.Session() as session, (output / "progress.jsonl").open("x") as progress:
        for task in tasks:
            result = {**task, "pages": [], "status": "complete", "items": 0}
            url, visited = task["url"], set()
            while url:
                url = safe_child_url(url, task["amendment_id"])
                if url in visited or len(visited) >= 10 or request_count >= 600:
                    result["status"] = "bounded_pagination_stop"
                    stop = request_count >= 600
                    break
                visited.add(url)
                time.sleep(max(0.0, 1.0 - (time.monotonic() - last_start)))
                last_start = time.monotonic()
                request_count += 1
                page = {"official_url": url, "observed_at": datetime.now(timezone.utc).isoformat()}
                try:
                    with session.get(url, params={"api_key": key, "limit": 250}, timeout=(5, 20), stream=True) as response:
                        page["http_status"] = response.status_code
                        raw = bytearray()
                        for chunk in response.iter_content(65536):
                            raw.extend(chunk)
                            if len(raw) > 2_000_000:
                                raise ValueError("page size bound exceeded")
                        page["response_sha256"] = hashlib.sha256(raw).hexdigest()
                        if response.status_code != 200:
                            result["status"] = "http_error"
                            stop = response.status_code == 429
                            result["pages"].append(page)
                            break
                        payload = json.loads(raw)
                        items = payload.get(task["payload_key"])
                        if not isinstance(items, list):
                            raise ValueError("expected child array absent")
                        filename = f"{task['amendment_id']}-{task['resource']}-{len(visited):03d}.json"
                        encoded = (json.dumps(sanitize(payload, key), ensure_ascii=False, indent=2) + "\n").encode()
                        (output / filename).write_bytes(encoded)
                        page.update({"file": filename, "sanitized_payload_sha256": hashlib.sha256(encoded).hexdigest(),
                                     "item_count": len(items), "pagination_count": payload.get("pagination", {}).get("count")})
                        result["items"] += len(items)
                        next_url = payload.get("pagination", {}).get("next")
                        url = safe_child_url(next_url, task["amendment_id"]) if next_url else None
                except (requests.RequestException, ValueError) as error:
                    result["status"] = "request_or_validation_error"
                    page["error_class"] = type(error).__name__
                    url = None
                result["pages"].append(page)
                if result["status"] != "complete":
                    break
            result["advertised_count_matches"] = (task["advertised_count"] is None or result["items"] == task["advertised_count"])
            completed.append(result)
            progress.write(json.dumps(result) + "\n")
            progress.flush()
            if len(completed) % 25 == 0 or len(completed) == len(tasks):
                print(json.dumps({"child_resources_completed": len(completed), "total": len(tasks),
                                  "requests": request_count, "errors": sum(r["status"] != "complete" for r in completed)}), flush=True)
            if stop:
                break
    report = {"schema_version": 1, "detail_report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
              "finished_at": datetime.now(timezone.utc).isoformat(), "tasks": len(tasks),
              "request_count": request_count, "results": completed, "unattempted": tasks[len(completed):],
              "production_modified": False, "applied_to_export": False,
              "scope": "Advertised API child arrays only; no document-body extraction or unrelated endpoint crawl."}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--details", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    capture(args.details, args.output)
