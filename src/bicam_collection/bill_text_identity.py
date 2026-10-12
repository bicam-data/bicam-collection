"""Conservative bill-rendition identity checks; never infer replacement links.

Only explicit opening document headings are evidence. References to other
Congresses or bills in substantive text are not document identities. Absence
of a recognizable heading is unknown, not a verified match or a conflict.
"""
from __future__ import annotations

import re
from urllib.parse import urlsplit


def inspect_bill_text_identity(url: str, text: str) -> dict:
    path = urlsplit(url).path.lower()
    expected = re.search(r"/(\d+)/bills/(hr|s|hres|sres|hjres|sjres|hconres|sconres)(\d+)(?:/|$)", path)
    if not expected:
        expected = re.search(r"bills-(\d+)(hr|s|hres|sres|hjres|sjres|hconres|sconres)(\d+)[a-z]", path)
    if not expected:
        return {"status": "not_applicable", "conflicts": []}
    congress, bill_type, number = expected.groups()
    # Require a standalone Congress heading near the beginning. Do not scan
    # the legislative body, where references to older Congresses are normal.
    opening = text[:1600]
    header = re.search(r"(?im)^\s*(\d+)(?:st|nd|rd|th|d)?\s+CONGRESS\s*$", opening)
    wrapper = re.search(r"\[Congressional Bills (\d+)(?:st|nd|rd|th|d)? Congress\]", opening, re.I)
    bill = re.search(r"(?im)^\s*(H\.\s*(?:R\.|(?:CON\.|J\.)?\s*RES\.)|S\.(?:\s*(?:CON\.|J\.)?\s*RES\.)?)\s*(\d+)\s*$", opening)
    observed_type = re.sub(r"[^a-z]", "", bill[1].lower()) if bill else None
    conflicts = []
    for role, value in (("body_congress", header[1] if header else None), ("wrapper_congress", wrapper[1] if wrapper else None)):
        if value and int(value) != int(congress):
            conflicts.append({"field": role, "expected": int(congress), "observed": int(value)})
    if bill and (observed_type, int(bill[2])) != (bill_type, int(number)):
        conflicts.append({"field": "body_bill", "expected": f"{bill_type}{int(number)}", "observed": f"{observed_type}{int(bill[2])}"})
    return {"status": "contradiction" if conflicts else "consistent_headings" if header and bill else "unverified",
            "expected_bill_id": f"{bill_type}{int(number)}-{int(congress)}",
            "observed_body_congress": int(header[1]) if header else None,
            "observed_body_bill": f"{observed_type}{int(bill[2])}" if bill else None,
            "conflicts": conflicts}
