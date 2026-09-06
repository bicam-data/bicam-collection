from __future__ import annotations

import unittest

from bicam_collection.urls import (
    canonicalize_congress_api_url,
    canonicalize_govinfo_url,
    congress_source_fallback_url,
    normalize_congress_treaty_committees_url,
    replace_query_params,
    with_query_params,
)


class UrlCanonicalizationTests(unittest.TestCase):
    def test_congress_query_parameter_order_is_canonical(self) -> None:
        first = canonicalize_congress_api_url(
            "https://api.congress.gov/v3/bill/97?offset=10000&limit=250&format=json"
        )
        second = canonicalize_congress_api_url(
            "https://api.congress.gov/v3/bill/97?limit=250&format=json&offset=10000"
        )

        self.assertEqual(first, second)
        self.assertEqual(
            first,
            "https://api.congress.gov/v3/bill/97?format=json&limit=250&offset=10000",
        )

    def test_congress_query_credentials_are_not_persisted(self) -> None:
        self.assertEqual(
            canonicalize_congress_api_url(
                "https://api.congress.gov/v3/bill/97?apiKey=secret&limit=250&format=xml"
            ),
            "https://api.congress.gov/v3/bill/97?format=json&limit=250",
        )

    def test_query_helpers_sort_added_parameters(self) -> None:
        url = "https://api.congress.gov/v3/bill/97?offset=10000"

        self.assertEqual(
            with_query_params(url, {"format": "json", "limit": 250}),
            "https://api.congress.gov/v3/bill/97?format=json&limit=250&offset=10000",
        )
        self.assertEqual(
            replace_query_params(url, {"format": "json", "limit": 250}),
            "https://api.congress.gov/v3/bill/97?format=json&limit=250&offset=10000",
        )

    def test_hearing_list_source_fallback_preserves_legacy_parameter_order(self) -> None:
        url = "https://api.congress.gov/v3/hearing?format=json&limit=250&offset=34250"
        self.assertEqual(
            congress_source_fallback_url(url, node_type="hearing_list"),
            "https://api.congress.gov/v3/hearing?offset=34250&limit=250&format=json",
        )
        self.assertIsNone(
            congress_source_fallback_url(url, node_type="bill_list")
        )

    def test_govinfo_query_parameter_order_is_canonical(self) -> None:
        first = canonicalize_govinfo_url(
            "https://api.govinfo.gov/collections/BILLS/2025-01-01/2025-01-02?pageSize=100&offsetMark=*&api_key=secret"
        )
        second = canonicalize_govinfo_url(
            "https://api.govinfo.gov/collections/BILLS/2025-01-01/2025-01-02?offsetMark=*&pageSize=100"
        )

        self.assertEqual(first, second)
        self.assertEqual(
            first,
            "https://api.govinfo.gov/collections/BILLS/2025-01-01/2025-01-02?offsetMark=%2A&pageSize=100",
        )

    def test_treaty_committees_route_preserves_partition_suffix(self) -> None:
        self.assertEqual(
            normalize_congress_treaty_committees_url(
                "https://api.congress.gov/v3/treaty/119/3/A/committees?format=json"
            ),
            "https://api.congress.gov/v3/treaty/119/3/A/committees?format=json",
        )

    def test_treaty_committees_route_preserves_unsuffixed_form(self) -> None:
        url = "https://api.congress.gov/v3/treaty/119/3/committees?format=json"
        self.assertEqual(normalize_congress_treaty_committees_url(url), url)


if __name__ == "__main__":
    unittest.main()
