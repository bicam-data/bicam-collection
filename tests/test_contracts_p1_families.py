from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class P1EndpointContractTests(unittest.TestCase):
    def test_law_contract(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "laws.yaml")
        payload = load_example_payload(PROJECT_ROOT / "api_schemas/laws/example_law.json")
        self.assertEqual(len(select(payload, spec.nodes["law_list"].item_path)), 1)
        self.assertTrue(select_one(payload, spec.nodes["law_list"].next_page_path))
        detail = load_example_payload(PROJECT_ROOT / "api_schemas/laws/example_lawdetail.json")
        self.assertEqual(select_one(detail, "$.bill.number"), "2808")
        self.assertTrue(spec.seed.requires_partition)

    def test_house_communication_contract(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs/house-communications.yaml")
        payload = load_example_payload(
            PROJECT_ROOT / "api_schemas/house-communications/example_house_communication.json"
        )
        self.assertEqual(len(select(payload, spec.nodes["house_communication_list"].item_path)), 1)
        self.assertTrue(select_one(payload, spec.nodes["house_communication_list"].next_page_path))
        detail = load_example_payload(
            PROJECT_ROOT / "api_schemas/house-communications/example_house_communicationdetail.json"
        )
        self.assertEqual(len(select(detail, "$.houseCommunication.matchingRequirements[*]")), 1)
        self.assertEqual(
            select_one(detail, "$.houseCommunication.matchingRequirements[*].url"),
            "https://api.congress.gov/v3/house-requirement/8070?format=json",
        )

    def test_senate_communication_contract(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs/senate-communications.yaml")
        payload = load_example_payload(
            PROJECT_ROOT / "api_schemas/senate-communications/example_senate_communication.json"
        )
        self.assertEqual(len(select(payload, spec.nodes["senate_communication_list"].item_path)), 1)
        self.assertTrue(select_one(payload, spec.nodes["senate_communication_list"].next_page_path))
        detail = load_example_payload(
            PROJECT_ROOT / "api_schemas/senate-communications/example_senate_communicationdetail.json"
        )
        self.assertEqual(select_one(detail, "$.senateCommunication.congress"), 119)

    def test_house_requirement_matching_communications_contract(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs/house-requirements.yaml")
        payload = load_example_payload(
            PROJECT_ROOT / "api_schemas/house-requirements/example_house_requirement.json"
        )
        self.assertEqual(len(select(payload, spec.nodes["house_requirement_list"].item_path)), 1)
        self.assertTrue(select_one(payload, spec.nodes["house_requirement_list"].next_page_path))
        detail = load_example_payload(
            PROJECT_ROOT / "api_schemas/house-requirements/example_house_requirementdetail.json"
        )
        self.assertEqual(select_one(detail, "$.houseRequirement.number"), 8070)
        self.assertEqual(
            spec.nodes["house_requirement_detail"].child_edges[0].fallback_url_suffix,
            "matching-communications",
        )
        matching = load_example_payload(
            PROJECT_ROOT / "api_schemas/house-requirements/example_matching_communications.json"
        )
        self.assertEqual(len(select(matching, spec.nodes["house_requirement_matching_communications"].item_path)), 1)
        self.assertTrue(
            select_one(matching, spec.nodes["house_requirement_matching_communications"].next_page_path)
        )


if __name__ == "__main__":
    unittest.main()
