from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.migrations import discover_migrations


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class MigrationTests(unittest.TestCase):
    def test_migrations_are_discovered_in_lexical_order(self) -> None:
        migrations = discover_migrations(PROJECT_ROOT / "migrations")
        self.assertEqual(
            [migration.name for migration in migrations],
            [
                "000_initial_schema.sql",
                "001_raw_response_versions.sql",
                "002_raw_response_current.sql",
                "003_backfill_fetch_version_links.sql",
                "004_expected_fetches.sql",
                "005_govinfo_collection_windows.sql",
                "006_raw_response_lookup_indexes.sql",
                "007_fetch_lineage_lookup_index.sql",
                "008_repair_raw_parent_ids.sql",
                "009_backfill_version_parent_ids.sql",
                "010_repair_derived_child_parent_ids.sql",
                "011_repair_bill_cosponsor_parent_ids.sql",
                "012_sync_repaired_version_parent_ids.sql",
                "013_html_text_versions.sql",
                "014_html_text_successful_current.sql",
                "015_committee_report_text_lineage.sql",
            ],
        )
        self.assertIn("raw_responses", migrations[0].sql)
        self.assertIn("raw_response_versions", migrations[1].sql)
        self.assertIn("raw_response_current", migrations[2].sql)
        self.assertIn("singleton_versions", migrations[3].sql)
        self.assertIn("crawl_run_expected_fetches", migrations[4].sql)
        self.assertIn("govinfo_collection_windows", migrations[5].sql)
        self.assertIn("idx_raw_response_versions_endpoint_url", migrations[6].sql)
        self.assertIn("idx_crawl_run_fetches_raw_response_version_id", migrations[6].sql)
        self.assertIn("idx_crawl_run_fetches_missing_raw_response_id", migrations[7].sql)
        self.assertIn("html_text_versions", migrations[13].sql)
        self.assertIn("html_text_successful_current", migrations[14].sql)
        self.assertIn("report_source_raw_response_version_id", migrations[15].sql)


if __name__ == "__main__":
    unittest.main()
