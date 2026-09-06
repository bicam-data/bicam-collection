from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "backfill_congress_govinfo_hearings.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_congress_govinfo_hearings", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_package_parser_maps_route_chamber_and_jacket_number() -> None:
    script = _load_module()
    assert script._parse_package("CHRG-119hhrg63835") == (119, "house", 63835)
    assert script._parse_package("CHRG-118shrg63648ih") == (118, "senate", 63648)
    assert script._parse_package("CHRG-117jhrg12345") == (117, "nochamber", 12345)
    assert script._parse_package("BILLS-119hr1ih") is None


def test_endpoint_uses_congress_hearing_shape() -> None:
    script = _load_module()
    hearing = script.GovInfoHearing(
        package_id="CHRG-119hhrg63835",
        parent_raw_id="00000000-0000-0000-0000-000000000001",
        parent_endpoint_url="https://api.govinfo.gov/packages/CHRG-119hhrg63835/summary",
        congress=119,
        route_chamber="house",
        jacket_number=63835,
    )
    assert hearing.endpoint_url == "https://api.congress.gov/v3/hearing/119/house/63835?format=json"


def test_joint_endpoint_uses_congress_legacy_nochamber_route() -> None:
    script = _load_module()
    hearing = script.GovInfoHearing(
        package_id="CHRG-117jhrg12345",
        parent_raw_id="00000000-0000-0000-0000-000000000001",
        parent_endpoint_url="https://api.govinfo.gov/packages/CHRG-117jhrg12345/summary",
        congress=117,
        route_chamber="nochamber",
        jacket_number=12345,
    )
    assert hearing.endpoint_url == (
        "https://api.congress.gov/v3/hearing/117/nochamber/12345?format=json"
    )


def test_loader_is_bounded_and_excludes_linked_and_known_404_records() -> None:
    script = _load_module()

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params):
            self.query = query
            self.params = params

            class Result:
                def fetchall(self):
                    return []

            return Result()

    connection = FakeConnection()
    with patch.object(script.psycopg, "connect", return_value=connection):
        script.load_missing(
            database_url="postgresql://example",
            congress_start=104,
            congress_end=119,
            limit=25,
        )

    assert "govinfo_hearing_to_congress_hearing" in connection.query
    assert "congress,route_chamber,jacket_number" in connection.query
    assert "when 'jhrg' then 'nochamber'" in connection.query
    assert "prior_failure.http_status = 404" in connection.query
    assert connection.params == (104, 119, 25)
