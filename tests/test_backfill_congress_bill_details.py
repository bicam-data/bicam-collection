from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "backfill_congress_bill_details.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_congress_bill_details", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_endpoint_uses_congress_bill_detail_shape() -> None:
    bill_type = _load_module().CongressBillDetail
    bill = bill_type(
        parent_raw_id="00000000-0000-0000-0000-000000000001",  # type: ignore[arg-type]
        parent_endpoint_url="https://api.govinfo.gov/packages/BILLS-117hconres110ih/summary",
        bill_type="hconres",
        bill_number=110,
        congress=117,
    )
    assert bill.endpoint_url == "https://api.congress.gov/v3/bill/117/hconres/110?format=json"


def test_loader_uses_only_missing_congress_matches_and_skips_404s() -> None:
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
        script.load_bill_details(
            database_url="postgresql://example",
            congresses=[117],
            limit=5,
            refresh=False,
        )

    assert "missing_congress_record" in connection.query
    assert "prior_failure.http_status = 404" in connection.query
    assert connection.params == [[117], 5]


def test_targeted_detail_backfill_immediately_traverses_child_resources() -> None:
    script = _load_module()
    bill = script.CongressBillDetail(
        parent_raw_id=UUID("00000000-0000-0000-0000-000000000001"),
        parent_endpoint_url="https://api.govinfo.gov/packages/BILLS-119hr10109ih/summary",
        bill_type="hr",
        bill_number=10109,
        congress=119,
    )
    seen_options = []

    class FakeClient:
        async def get_json(self, endpoint_url, **kwargs):
            return SimpleNamespace(
                payload={"bill": {"congress": 119, "type": "HR", "number": "10109"}},
                metadata=SimpleNamespace(
                    status_code=200,
                    duration_ms=1,
                    attempt_count=1,
                    rate_limit_attempts=0,
                    api_key_name="redacted",
                    pool_allocation=None,
                ),
            )

    class FakeStore:
        async def start_run(self, category, mode):
            return "00000000-0000-0000-0000-000000000010"

        async def upsert_response(self, **kwargs):
            return SimpleNamespace(
                raw_response_id="00000000-0000-0000-0000-000000000011",
                raw_response_version_id="00000000-0000-0000-0000-000000000012",
            )

        async def record_fetch(self, **kwargs):
            return None

        async def record_failed_request(self, **kwargs):
            return None

        async def finish_run(self, *args, **kwargs):
            return None

        async def close(self):
            return None

    fake_store = FakeStore()

    class FakeCrawler:
        def __init__(self, **kwargs):
            assert kwargs["concurrency"] == 1
            assert kwargs["store"] is fake_store

        async def run(self, options):
            seen_options.append(options)
            return "00000000-0000-0000-0000-000000000013"

    args = script.parse_args(["--limit", "1"])
    with (
        patch.object(script.Settings, "from_env", return_value=SimpleNamespace(database_url="db", env_path=Path(".env"))),
        patch.object(script, "load_bill_details", return_value=[bill]),
        patch.object(script, "load_spec", return_value=object()),
        patch.object(script, "base_url_for_spec", return_value="https://api.congress.gov/v3"),
        patch.object(script, "build_client", return_value=FakeClient()),
        patch.object(script, "AsyncPostgresStore", return_value=fake_store),
        patch.object(script, "AsyncSpecCrawler", FakeCrawler),
    ):
        succeeded, failed = asyncio.run(script.run_backfill(args, project_root=PROJECT_ROOT))

    assert (succeeded, failed) == (1, 0)
    assert len(seen_options) == 1
    assert seen_options[0].mode == "backfill_descendants"
    assert seen_options[0].seed_node_type == "bill_detail"
    assert seen_options[0].seed_path.endswith("/bill/119/hr/10109?format=json")
