from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "backfill_congress_committee_reports.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_congress_committee_reports", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_loader_prioritizes_open_source_gap_reviews() -> None:
    script = _load_module()

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, _params):
            self.query = query
            return self

        def fetchall(self):
            return []

    connection = FakeConnection()
    with patch.object(script.psycopg, "connect", return_value=connection):
        assert script._rows(database_url="postgresql://example", congresses=None, limit=10, refresh=False) == []

    assert "fct_reconciliation_exceptions" in connection.query
    assert "exception_status = 'open'" in connection.query
    assert "left join (" in connection.query
    assert "then 0 else 1 end" in connection.query
