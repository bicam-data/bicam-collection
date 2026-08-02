"""Load a tiny, deterministic raw corpus for CI's database-backed dbt build."""

from __future__ import annotations

import hashlib
import json
import os
import uuid

import psycopg


def _fixture_rows() -> list[tuple[str, str, str, str, dict]]:
    return [
        (
            "https://api.congress.gov/v3/bill/118/hr/1?format=json",
            "congress",
            "bills",
            "bill_detail",
            {
                "bill": {
                    "type": "HR",
                    "number": "1",
                    "congress": 118,
                    "title": "A bill for the people",
                    "originChamber": "House",
                    "introducedDate": "2023-01-03",
                    "updateDate": "2023-01-04T00:00:00Z",
                    "actions": {"count": 0},
                    "amendments": {"count": 0},
                    "committees": {"count": 0},
                    "cosponsors": {"count": 0},
                    "relatedBills": {"count": 0},
                    "subjects": {"count": 0},
                    "summaries": {"count": 0},
                    "textVersions": {"count": 0},
                    "titles": {"count": 0},
                }
            },
        ),
        (
            "https://api.congress.gov/v3/committee-report/118/hrpt/1?format=json",
            "congress",
            "committee-reports",
            "committee_report_detail",
            {
                "committeeReports": [
                    {
                        "type": "hrpt",
                        "number": 1,
                        "part": 1,
                        "congress": 118,
                        "sessionNumber": 1,
                        "title": "A committee report for the people",
                        "chamber": "House",
                        "isConferenceReport": False,
                        "citation": "H. Rept. 118-1",
                        "issueDate": "2023-01-04T00:00:00Z",
                        "text": {"count": 0},
                        "updateDate": "2023-01-04T00:00:00Z",
                    }
                ]
            },
        ),
        (
            "https://api.govinfo.gov/packages/BILLS-118hr1ih/summary?api_key=ci",
            "govinfo",
            "govinfo-bills",
            "govinfo_bill_package_detail",
            {
                "packageId": "BILLS-118hr1ih",
                "billType": "HR",
                "billNumber": 1,
                "congress": 118,
                "billVersion": "ih",
                "title": "A bill for the people",
                "originChamber": "HOUSE",
                "currentChamber": "HOUSE",
                "dateIssued": "2023-01-03",
                "lastModified": "2023-01-04T00:00:00Z",
                "collectionCode": "BILLS",
            },
        ),
        (
            "https://api.govinfo.gov/packages/CRPT-118hrpt1/summary?api_key=ci",
            "govinfo",
            "govinfo-committee-reports",
            "govinfo_committee_report_package_detail",
            {
                "packageId": "CRPT-118hrpt1",
                "docClass": "hrpt",
                "documentNumber": "1",
                "congress": 118,
                "session": 1,
                "title": "A committee report for the people",
                "chamber": "HOUSE",
                "dateIssued": "2023-01-04",
                "lastModified": "2023-01-04T00:00:00Z",
                "collectionCode": "CRPT",
            },
        ),
    ]


def main() -> int:
    if os.getenv("BICAM_CI_FIXTURES") != "1":
        raise SystemExit("refusing to load fixtures without BICAM_CI_FIXTURES=1")

    database_url = os.environ["DATABASE_URL"]
    with psycopg.connect(database_url) as conn:
        for endpoint_url, source, category, node_type, response in _fixture_rows():
            raw_id = uuid.uuid5(uuid.NAMESPACE_URL, endpoint_url)
            response_json = json.dumps(response, sort_keys=True)
            response_hash = hashlib.md5(response_json.encode(), usedforsecurity=False).hexdigest()
            conn.execute(
                """
                INSERT INTO raw_responses (
                    id, endpoint_url, source, category, node_type, response,
                    response_hash, fetched_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW())
                ON CONFLICT (endpoint_url) DO UPDATE SET
                    source = EXCLUDED.source,
                    category = EXCLUDED.category,
                    node_type = EXCLUDED.node_type,
                    response = EXCLUDED.response,
                    response_hash = EXCLUDED.response_hash,
                    fetched_at = EXCLUDED.fetched_at
                """,
                (raw_id, endpoint_url, source, category, node_type, response_json, response_hash),
            )
            conn.execute(
                """
                INSERT INTO raw_response_versions (
                    id, raw_response_id, endpoint_url, source, category, node_type,
                    response, response_hash, first_seen_at, last_seen_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW(), NOW())
                ON CONFLICT (raw_response_id, response_hash) DO NOTHING
                """,
                (
                    uuid.uuid5(uuid.NAMESPACE_URL, f"version:{endpoint_url}:{response_hash}"),
                    raw_id,
                    endpoint_url,
                    source,
                    category,
                    node_type,
                    response_json,
                    response_hash,
                ),
            )
        conn.commit()
    print(f"loaded {len(_fixture_rows())} CI fixture payloads")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
