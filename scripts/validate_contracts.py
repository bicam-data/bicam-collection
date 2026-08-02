from __future__ import annotations

from pathlib import Path

from bicam_collection.contracts import (
    load_contract_document,
    validate_collection_scope,
    validate_source_strategy,
)
from bicam_collection.spec_loader import discover_spec_categories


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    categories = set(discover_spec_categories(project_root / "specs"))
    strategy = load_contract_document(project_root / "contracts" / "source_strategy.yaml")
    scope = load_contract_document(project_root / "contracts" / "collection_scope.yaml")
    validate_source_strategy(strategy, categories)
    validate_collection_scope(scope, categories)
    print(f"validated source strategy and bounded scope for {len(categories)} spec categories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
