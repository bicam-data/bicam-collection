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
            ],
        )
        self.assertIn("raw_responses", migrations[0].sql)
        self.assertIn("raw_response_versions", migrations[1].sql)
        self.assertIn("raw_response_current", migrations[2].sql)
        self.assertIn("singleton_versions", migrations[3].sql)


if __name__ == "__main__":
    unittest.main()
