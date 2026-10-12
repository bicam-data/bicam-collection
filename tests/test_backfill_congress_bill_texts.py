from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "backfill_congress_bill_texts.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_congress_bill_texts", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class BackfillCongressBillTextsTests(unittest.TestCase):
    def test_bill_text_builds_bounded_congress_text_url(self) -> None:
        script = _load_module()

        bill_text = script.BillText(
            parent_raw_id="00000000-0000-0000-0000-000000000001",
            parent_endpoint_url="https://api.congress.gov/v3/bill/119/hr/1?format=json",
            bill_type="hr",
            bill_number=1,
            congress=119,
        )

        self.assertEqual(
            bill_text.endpoint_url,
            "https://api.congress.gov/v3/bill/119/hr/1/text?format=json&limit=250",
        )

if __name__ == "__main__":
    unittest.main()
