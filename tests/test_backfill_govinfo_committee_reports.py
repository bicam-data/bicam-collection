from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "backfill_govinfo_committee_reports.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_govinfo_committee_reports", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class BackfillGovinfoCommitteeReportsTests(unittest.TestCase):
    def test_report_set_builds_govinfo_package_identity(self) -> None:
        script = _load_module()

        report_set = script.ReportSet(report_type="hrpt", report_number=562, congress=116)

        self.assertEqual(report_set.package_id, "CRPT-116hrpt562")
        self.assertEqual(
            report_set.package_url,
            "https://api.govinfo.gov/packages/CRPT-116hrpt562/summary",
        )

    def test_retry_404_window_must_be_positive(self) -> None:
        script = _load_module()

        with self.assertRaises(ValueError):
            script.load_report_sets(
                database_url="postgresql://unused",
                congresses=[119],
                limit=1,
                refresh=False,
                retry_404_after_hours=0,
            )


if __name__ == "__main__":
    unittest.main()
