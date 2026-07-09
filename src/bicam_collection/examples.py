from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


_FENCE_RE = re.compile(r"```json\s*(.*?)```", re.DOTALL)


def _extract_json_block(raw_text: str) -> str:
    match = _FENCE_RE.search(raw_text)
    return match.group(1).strip() if match else raw_text.strip()


def _repair_json(name: str, raw_json: str) -> str:
    repaired = raw_json
    repaired = repaired.replace(']\n    {\n  "pagination"', '],\n  "pagination"')
    repaired = repaired.replace('],\n{\n  "pagination"', '],\n  "pagination"')
    repaired = repaired.replace('}\n    "policyArea"', '},\n    "policyArea"')
    if name == "example_bill_text.md":
        repaired = repaired.replace('    ],\n    {\n  "pagination"', '    ],\n  "pagination"')
    if name == "example_bill_title.md":
        repaired = repaired.replace('],\n{\n  "pagination"', '],\n  "pagination"')
    return repaired


def load_example_payload(path: Path) -> Any:
    raw_text = path.read_text(encoding="utf-8")
    raw_json = _extract_json_block(raw_text)
    repaired = _repair_json(path.name, raw_json)
    return json.loads(repaired)

