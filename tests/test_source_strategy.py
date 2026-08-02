from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
