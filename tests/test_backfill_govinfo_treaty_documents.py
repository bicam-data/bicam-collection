from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from unittest.mock import patch

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts/backfill_govinfo_treaty_documents.py"
SPEC = importlib.util.spec_from_file_location("backfill_govinfo_treaty_documents", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)


def test_treaty_package_id_uses_received_congress_and_number() -> None:
    assert module.treaty_package_id(congress_received=119, treaty_number=3) == "CDOC-119tdoc3"
    assert module.TreatyPackage(119, 3).package_url.endswith("/CDOC-119tdoc3/summary")


def test_treaty_package_id_rejects_nonpositive_identity() -> None:
    with pytest.raises(ValueError):
        module.treaty_package_id(congress_received=0, treaty_number=3)


def test_granule_links_are_canonicalized_and_next_page_is_supported() -> None:
    payload = {
        "granules": [
            {"granuleLink": "https://api.govinfo.gov/packages/CDOC-119tdoc3/granules/a/summary?api_key=secret"},
            {"granuleLink": None},
            "not-an-object",
        ],
        "nextPage": "https://api.govinfo.gov/packages/CDOC-119tdoc3/granules?offsetMark=abc&api_key=secret",
    }
    assert module._granule_detail_urls(payload) == [
        "https://api.govinfo.gov/packages/CDOC-119tdoc3/granules/a/summary"
    ]
    assert module._next_page(payload) == (
        "https://api.govinfo.gov/packages/CDOC-119tdoc3/granules?offsetMark=abc"
    )


def test_loader_targets_only_missing_packages_and_preserves_404_ttl() -> None:
    class FakeResult:
        def fetchall(self):
            return [(119, 3), (118, 1)]

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params):
            self.query = query
            self.params = params
            return FakeResult()

    connection = FakeConnection()
    with patch.object(module.psycopg, "connect", return_value=connection):
        packages = module.load_treaty_packages(
            database_url="postgresql://unused",
            congresses=[119],
            limit=25,
            refresh=False,
            retry_404_after_hours=72.0,
        )

    assert [(item.congress_received, item.treaty_number) for item in packages] == [(119, 3), (118, 1)]
    assert "stg_congress__treaty_detail" in connection.query
    assert "CDOC-" in connection.query
    assert "from failed_requests as prior_failure" in connection.query
    assert "interval '72.000000 hours'" in connection.query
    assert connection.params == [[119], 25]


def test_dry_run_argument_is_available_and_defaults_to_bounded_batch() -> None:
    args = module.parse_args(["--dry-run", "--limit", "7", "--max-pages", "4"])
    assert args.dry_run is True
    assert args.limit == 7
    assert args.max_pages == 4
