from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "backfill_govinfo_bills.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_govinfo_bills", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class BackfillGovinfoBillsTests(unittest.TestCase):
    def test_bill_package_builds_govinfo_summary_url(self) -> None:
        script = _load_module()

        package = script.BillPackage(package_id="BILLS-119hr7147ih")

        self.assertEqual(
            package.package_url,
            "https://api.govinfo.gov/packages/BILLS-119hr7147ih/summary",
        )

    def test_default_request_concurrency_is_bounded(self) -> None:
        script = _load_module()

        args = script.parse_args([])

        self.assertEqual(args.concurrency, 3)
        self.assertEqual(args.limit, 1000)


if __name__ == "__main__":
    unittest.main()
