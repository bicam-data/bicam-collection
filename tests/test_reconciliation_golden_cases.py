from __future__ import annotations

import csv
from pathlib import Path
import unittest

from bicam_collection.contracts import (
    render_canonical_id,
    validate_entity_contract_registry,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CASES_PATH = PROJECT_ROOT / "tests" / "fixtures" / "reconciliation_golden_cases.csv"


class ReconciliationGoldenCaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contracts = validate_entity_contract_registry(PROJECT_ROOT / "contracts" / "entities")

    def test_canonical_id_and_decision_fixtures(self) -> None:
        with CASES_PATH.open(newline="", encoding="utf-8") as handle:
            cases = list(csv.DictReader(handle))
        self.assertGreaterEqual(len(cases), 7)
        for case in cases:
            contract = self.contracts[case["entity"]]
            values = {key: value for key, value in case.items() if value != ""}
            values["is_errata"] = case["is_errata"].lower() == "true"
            canonical = render_canonical_id(contract, values)
            self.assertEqual(canonical, case["expected_canonical_id"], case["case_id"])
            if case["source_presence"] == "both":
                self.assertEqual(case["expected_decision"], "accepted", case["case_id"])
            elif case["source_presence"] == "both_duplicate":
                self.assertEqual(case["expected_decision"], "ambiguous", case["case_id"])
            else:
                self.assertEqual(case["expected_decision"], "unmatched", case["case_id"])


if __name__ == "__main__":
    unittest.main()
