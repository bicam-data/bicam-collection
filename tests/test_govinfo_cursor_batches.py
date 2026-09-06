from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import patch

import pytest


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_govinfo_cursor_batches.py"
    spec = importlib.util.spec_from_file_location("run_govinfo_cursor_batches", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cursor_runner_redacts_query_credentials() -> None:
    module = _load_module()
    value = "request failed https://example.test/items?api_key=secret-value&offsetMark=cursor"

    assert module.redact_output(value) == (
        "request failed https://example.test/items?api_key=[REDACTED]&offsetMark=cursor"
    )


def test_cursor_runner_rejects_unbounded_or_invalid_values() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--category",
            "govinfo-bills",
            "--seed-path",
            "/collections/BILLS",
            "--seed-node-type",
            "govinfo_bill_package_list",
            "--batches",
            "0",
        ]
    )

    with pytest.raises(ValueError, match="batches must be positive"):
        module._ensure_positive(args)


def test_cursor_runner_defaults_are_bounded_by_pacer() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--category",
            "govinfo-bills",
            "--seed-path",
            "/collections/BILLS",
            "--seed-node-type",
            "govinfo_bill_package_list",
        ]
    )

    assert args.concurrency == 3
    assert args.request_interval == 0.25


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (None, (False, None)),
        ((None,), (True, None)),
        (("https://api.govinfo.gov/collections/CHRG?offsetMark=next",),
         (True, "https://api.govinfo.gov/collections/CHRG?offsetMark=next")),
    ],
)
def test_latest_completed_seed_preserves_terminal_cursor_state(row, expected) -> None:
    module = _load_module()

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params):
            self.query = query
            self.params = params

            class Result:
                def fetchone(self):
                    return row

            return Result()

    connection = FakeConnection()
    with patch.object(module.psycopg, "connect", return_value=connection):
        result = module._latest_completed_seed(
            "postgresql://example",
            category="govinfo-hearings",
            node_type="govinfo_hearing_package_list",
        )

    assert result == expected
    assert "runs.status = 'completed'" in connection.query
    assert "versions.response->>'nextPage' is not null" not in connection.query
    assert connection.params == ("govinfo-hearings", "govinfo_hearing_package_list")


def test_cursor_runner_requires_seed_or_completed_resume() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--category",
            "govinfo-hearings",
            "--seed-node-type",
            "govinfo_hearing_package_list",
        ]
    )

    with pytest.raises(ValueError, match="seed-path is required"):
        module._ensure_positive(args)
