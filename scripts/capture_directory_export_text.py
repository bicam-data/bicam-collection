"""Capture the official 119th-Congress directory text for structured-field work.

One bounded text download, plus two read-only current raw-source observations.
No extraction is published and no prior-Congress member data is copied forward.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import psycopg
import requests

from bicam_collection.config import Settings
from project_hearing_export_children import assert_no_credentials

URL = "https://www.govinfo.gov/content/pkg/CDIR-2026-02-20/text/CDIR-2026-02-20.txt"
RAW_URLS = ["https://api.govinfo.gov/packages/CDIR-2026-02-20/summary",
            "https://api.govinfo.gov/packages/CDIR-2026-02-20/granules?offsetMark=%2A&pageSize=100"]


def capture(output):
    with requests.get(URL, timeout=(5, 30), stream=True, allow_redirects=False) as response:
        if response.status_code != 200:
            raise ValueError(f"official directory text HTTP {response.status_code}")
        raw = bytearray()
        for chunk in response.iter_content(65536):
            raw.extend(chunk)
            if len(raw) > 2_000_000:
                raise ValueError("official directory text exceeds 2 MB bound")
        headers = {k: response.headers[k] for k in ("Content-Type", "Content-Length", "Last-Modified", "ETag") if k in response.headers}
    text = raw.decode("utf-8")
    if len(text) < 100_000 or ("119th" not in text and "Nineteenth" not in text):
        raise ValueError("download does not have expected directory content")
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    with psycopg.connect(settings.database_url, options=("-c default_transaction_read_only=on -c statement_timeout=8000 "
                                                        "-c lock_timeout=1000 -c max_parallel_workers_per_gather=0")) as connection:
        rows = connection.execute("""
            select endpoint_url, response_hash, response, fetched_at
            from raw_responses where endpoint_url = any(%s)
        """, (RAW_URLS,)).fetchall()
    if len(rows) != 2:
        raise ValueError("expected exact package and granule-list observations")
    observations = []
    for url, response_hash, payload, fetched_at in rows:
        assert_no_credentials(payload)
        observations.append({"endpoint_url": url, "raw_response_hash": response_hash,
                             "fetched_at": fetched_at.isoformat(), "payload": payload})
    output.mkdir(parents=True, exist_ok=False)
    with (output / "CDIR-2026-02-20.txt").open("xb") as stream:
        stream.write(raw)
    report = {"schema_version": 1, "observed_at": datetime.now(timezone.utc).isoformat(),
        "official_text_url": URL, "official_details_url": "https://www.govinfo.gov/app/details/CDIR-2026-02-20",
        "file": "CDIR-2026-02-20.txt", "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
        "response_headers": headers, "raw_observations": observations,
        "production_modified": False, "applied_to_export": False,
        "scope": "119th-Congress structured member/term feature extraction evidence only; document body excluded from public Parquet.",
        "limitations": ["Current download and current retained observations, not an immutable pre-snapshot document version.",
                        "Structured field extraction, member matching and fidelity validation are not yet implemented."]}
    with (output / "report.json").open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = capture(args.output)
    print(json.dumps({"file": report["file"], "bytes": report["bytes"], "sha256": report["sha256"]}))
