from __future__ import annotations

import csv
import unittest
from pathlib import Path

from bicam_collection.contracts import (
    load_contract_document,
    validate_collection_scope,
    validate_source_strategy,
)
from bicam_collection.spec_loader import discover_spec_categories


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SourceStrategyTests(unittest.TestCase):
    def test_source_strategy_covers_every_spec_category(self) -> None:
        strategy = load_contract_document(PROJECT_ROOT / "contracts" / "source_strategy.yaml")
        validate_source_strategy(strategy, set(discover_spec_categories(PROJECT_ROOT / "specs")))

    def test_collection_scope_is_bounded_and_known(self) -> None:
        strategy = load_contract_document(PROJECT_ROOT / "contracts" / "source_strategy.yaml")
        scope = load_contract_document(PROJECT_ROOT / "contracts" / "collection_scope.yaml")
        validate_collection_scope(scope, set(strategy["categories"]))

    def test_general_coverage_rules_cover_every_spec_category(self) -> None:
        spec_categories = set(discover_spec_categories(PROJECT_ROOT / "specs"))
        with (PROJECT_ROOT / "dbt_project/seeds/operations/collection_coverage_rules.csv").open(
            newline="", encoding="utf-8"
        ) as handle:
            rule_categories = {row["category"] for row in csv.DictReader(handle)}
        self.assertEqual(rule_categories, spec_categories)

    def test_full_catalog_rules_have_declared_scope_rows(self) -> None:
        with (PROJECT_ROOT / "dbt_project/seeds/operations/collection_coverage_rules.csv").open(
            newline="", encoding="utf-8"
        ) as handle:
            full_categories = {
                row["category"]
                for row in csv.DictReader(handle)
                if row["expected_scope"] == "full_congress_scope"
            }
        with (PROJECT_ROOT / "dbt_project/seeds/operations/collection_scope_partitions.csv").open(
            newline="", encoding="utf-8"
        ) as handle:
            scoped_categories = {row["category"] for row in csv.DictReader(handle)}
        self.assertLessEqual(full_categories, scoped_categories)


if __name__ == "__main__":
    unittest.main()
