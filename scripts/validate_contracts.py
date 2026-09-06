from __future__ import annotations

from pathlib import Path

from bicam_collection.contracts import (
    load_contract_document,
    validate_entity_contract_registry,
    validate_collection_scope,
    validate_schedule,
    validate_source_strategy,
)
from bicam_collection.spec_loader import discover_spec_categories


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    categories = set(discover_spec_categories(project_root / "specs"))
    strategy = load_contract_document(project_root / "contracts" / "source_strategy.yaml")
    scope = load_contract_document(project_root / "contracts" / "collection_scope.yaml")
    schedule = load_contract_document(project_root / "contracts" / "schedules.yaml")
    entities = validate_entity_contract_registry(project_root / "contracts" / "entities")
    validate_source_strategy(strategy, categories)
    validate_collection_scope(scope, categories)
    validate_schedule(schedule, categories)
    for category, config in strategy["categories"].items():
        entity_contract = entities.get(config["entity"])
        if entity_contract is None:
            continue
        authoritative_source = config["authoritative_source"]
        if authoritative_source not in entity_contract["sources"]:
            raise ValueError(
                f"source strategy authority is absent from entity contract: {category} -> {authoritative_source}"
            )
        if entity_contract["workflow"] != config["workflow"] and not (
            config["workflow"] == "evidence_ingestion" and entity_contract["workflow"] == "reconciliation"
        ):
            raise ValueError(f"source strategy workflow disagrees with entity contract: {category}")
    print(
        f"validated {len(entities)} entity contracts, source strategy, "
        f"and historical scope for {len(categories)} spec categories"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
