from __future__ import annotations

import importlib.util
import sys
from unittest.mock import patch
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

    def test_introduced_package_id_uses_origin_chamber_suffix(self) -> None:
        script = _load_module()

        self.assertEqual(
            script.introduced_package_id(bill_type="hconres", bill_number=10, congress=103),
            "BILLS-103hconres10ih",
        )
        self.assertEqual(
            script.introduced_package_id(bill_type="sres", bill_number=3, congress=119),
            "BILLS-119sres3is",
        )

    def test_default_request_concurrency_is_bounded(self) -> None:
        script = _load_module()

        args = script.parse_args([])

        self.assertEqual(args.concurrency, 3)
        self.assertEqual(args.limit, 1000)
        self.assertEqual(args.request_interval, 0.25)
        self.assertEqual(args.key_wait_timeout, 180.0)
        self.assertEqual(args.retry_404_after_hours, 168.0)

    def test_modern_missing_mode_targets_congress_only_records(self) -> None:
        script = _load_module()

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params):
                self.query = query
                self.params = params

                class Result:
                    def fetchall(self):
                        return [("BILLS-119hr10ih",)]

                return Result()

        connection = FakeConnection()
        with patch.object(script.psycopg, "connect", return_value=connection):
            packages = script.load_bill_packages(
                database_url="postgresql://example",
                congresses=[119],
                limit=5,
                refresh=False,
                modern_missing=True,
            )

        self.assertEqual([package.package_id for package in packages], ["BILLS-119hr10ih"])
        self.assertIn("int_bills__congress", connection.query)
        self.assertIn("congresses.congress >= 103", connection.query)
        self.assertIn("lower(coalesce(congresses.title, '')) not like 'reserved%%'", connection.query)
        self.assertIn("congresses.congress = any(%s)", connection.query)
        self.assertIn("interval '168.000000 hours'", connection.query)
        self.assertIn("prior_failure.http_status = 404", connection.query)
        self.assertEqual(connection.params, [[119], 5])

    def test_unmatched_congress_filter_binds_query_parameter(self) -> None:
        script = _load_module()

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params):
                self.query = query
                self.params = params
                class Result:
                    def fetchall(self):
                        return [("BILLS-103hr1ih",)]

                return Result()

        connection = FakeConnection()
        with patch.object(script.psycopg, "connect", return_value=connection):
            packages = script.load_bill_packages(
                database_url="postgresql://example",
                congresses=[103],
                limit=5,
                refresh=False,
                unmatched=True,
            )

        self.assertEqual([package.package_id for package in packages], ["BILLS-103hr1ih"])
        self.assertIn("parsed.congress = any(%s)", connection.query)
        self.assertIn("prior_failure.http_status = 404", connection.query)
        self.assertEqual(connection.params, [[103], 5])


if __name__ == "__main__":
    unittest.main()
