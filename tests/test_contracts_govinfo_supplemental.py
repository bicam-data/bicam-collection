from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SupplementalGovInfoSpecTests(unittest.TestCase):
    def test_hearing_spec_has_package_and_granule_traversal(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "govinfo-hearings.yaml")
        self.assertEqual(spec.seed.node_type, "govinfo_hearing_package_list")
        self.assertEqual(spec.nodes["govinfo_hearing_package_list"].child_edges[0].target, "govinfo_hearing_package_detail")
        self.assertEqual(spec.nodes["govinfo_hearing_package_detail"].child_edges[0].target, "govinfo_hearing_granule_list")

    def test_committee_print_spec_has_package_and_granule_traversal(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "govinfo-committee-prints.yaml")
        self.assertEqual(spec.seed.node_type, "govinfo_committee_print_package_list")
        self.assertEqual(
            spec.nodes["govinfo_committee_print_package_detail"].child_edges[0].target,
            "govinfo_committee_print_granule_list",
        )

    def test_congressional_directory_spec_has_member_granule_traversal(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "govinfo-congressional-directories.yaml")
        self.assertEqual(
            spec.nodes["govinfo_congressional_directory_package_detail"].child_edges[0].target,
            "govinfo_congressional_directory_granule_list",
        )
        granules = spec.nodes["govinfo_congressional_directory_granule_list"]
        self.assertEqual(granules.next_page_path, "$.nextPage")
        self.assertEqual(
            granules.child_edges[0].target,
            "govinfo_congressional_directory_granule_detail",
        )


if __name__ == "__main__":
    unittest.main()
