from __future__ import annotations

import unittest
from pathlib import Path

import yaml

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "house-votes"
SPEC = load_spec(PROJECT_ROOT / "specs" / "house-votes.yaml")


class HouseVoteExampleContractTests(unittest.TestCase):
    def test_beta_coverage_is_explicitly_limited(self) -> None:
        raw = yaml.safe_load((PROJECT_ROOT / "specs" / "house-votes.yaml").read_text())
        coverage = raw["coverage"]
        self.assertEqual(coverage["congresses"], [118, 119])
        self.assertNotIn(1, coverage["congresses"])
        self.assertEqual(coverage["scope"], "documented_beta")

    def test_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_house_vote.json")
        self.assertEqual(len(select(payload, SPEC.nodes["house_vote_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["house_vote_list"].next_page_path))
        self.assertEqual(select_one(payload, "$.houseRollCallVotes[*].congress"), 119)

    def test_detail_and_members_contracts(self) -> None:
        detail = load_example_payload(EXAMPLES_DIR / "example_house_votedetail.json")
        members = load_example_payload(EXAMPLES_DIR / "example_house_vote_members.json")
        self.assertEqual(select_one(detail, "$.houseRollCallVote.rollCallNumber"), 17)
        self.assertEqual(len(select(members, SPEC.nodes["house_vote_members"].item_path)), 1)
        self.assertEqual(select_one(members, "$.houseRollCallVoteMemberVotes.results[*].bioguideID"), "A000055")

    def test_members_edge_uses_documented_fallback(self) -> None:
        edge = SPEC.nodes["house_vote_detail"].child_edges[0]
        self.assertEqual(edge.target, "house_vote_members")
        self.assertEqual(edge.fallback_url_suffix, "members")


if __name__ == "__main__":
    unittest.main()
