from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.contracts import load_contract_document, validate_entity_contract_registry


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class EntityContractTests(unittest.TestCase):
    def test_entity_registry_is_structurally_valid(self) -> None:
        contracts = validate_entity_contract_registry(PROJECT_ROOT / "contracts" / "entities")
        self.assertEqual(
            set(contracts),
            {
                "amendment",
                "bill",
                "committee",
                "committee_meeting",
                "committee_print",
                "committee_report",
                "congress",
                "hearing",
                "member",
                "nomination",
                "treaty",
            },
        )

    def test_canonical_columns_are_declared_for_every_entity(self) -> None:
        contracts = validate_entity_contract_registry(PROJECT_ROOT / "contracts" / "entities")
        for contract in contracts.values():
            self.assertTrue(contract["identity"]["canonical_column"].startswith("canonical_"))

    def test_congress_ingestion_entities_have_single_authoritative_source(self) -> None:
        for entity in (
            "committee",
            "committee_meeting",
            "amendment",
            "nomination",
            "member",
        ):
            contract = load_contract_document(PROJECT_ROOT / "contracts" / "entities" / f"{entity}.yaml")
            self.assertEqual(contract["workflow"], "ingestion_only")
            self.assertEqual(set(contract["sources"]), {"congress"})
            self.assertTrue(contract["identity"]["components"])
            self.assertTrue(contract["sources"]["congress"]["natural_key"])

    def test_congress_directory_evidence_is_declared(self) -> None:
        contract = load_contract_document(PROJECT_ROOT / "contracts" / "entities" / "congress.yaml")
        self.assertEqual(contract["workflow"], "reconciliation")
        self.assertEqual(set(contract["sources"]), {"congress", "govinfo"})
        self.assertEqual(
            contract["sources"]["govinfo"]["model"],
            "stg_govinfo__congressional_directories",
        )

    def test_hearing_contract_keeps_joint_as_a_chamber_value(self) -> None:
        contract = load_contract_document(PROJECT_ROOT / "contracts" / "entities" / "hearing.yaml")
        chamber = contract["classification"]["chamber"]
        self.assertEqual(chamber["allowed_values"], ["house", "senate", "joint"])
        self.assertFalse(chamber["joint_is_separate_entity"])
        self.assertEqual(set(contract["sources"]), {"congress", "govinfo"})


if __name__ == "__main__":
    unittest.main()
