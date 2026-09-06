from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "backfill_congress_root_details.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_congress_root_details", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_loader_accepts_global_list_routes_and_scopes_by_item_congress() -> None:
    script = _load_module()

    class Result:
        def fetchall(self):
            return [
                (
                    "parent-raw-id",
                    "https://api.congress.gov/v3/amendment?offset=1&limit=250",
                    "https://api.congress.gov/v3/amendment/100/hamdt/100",
                )
            ]

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params):
            self.query = query
            self.params = params
            return Result()

    connection = FakeConnection()
    with patch.object(script.psycopg, "connect", return_value=connection):
        details = script.load_missing(
            database_url="postgresql://example",
            category="amendments",
            congress_start=100,
            congress_end=100,
            limit=10,
        )

    assert len(details) == 1
    assert details[0].endpoint_url == "https://api.congress.gov/v3/amendment/100/hamdt/100?format=json"
    assert "lists.endpoint_url ~" not in connection.query
    assert connection.params[2:5] == ("amendments", "amendments", "amendment_list")
    assert connection.params[-1] == 10
    assert "source_failures.attempts" in connection.query
    assert connection.params[-2] == 3


def test_source_error_attempt_limit_is_configurable() -> None:
    script = _load_module()

    args = script.parse_args(
        [
            "--category",
            "bills",
            "--congress-start",
            "103",
            "--congress-end",
            "119",
            "--max-source-error-attempts",
            "5",
        ]
    )

    assert args.max_source_error_attempts == 5
