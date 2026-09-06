from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_root_gap_repair.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("run_root_gap_repair", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_load_gaps_uses_exact_actionable_position_ranges() -> None:
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
                    return [("hearings", "109", 2890, 2891)]

            return Result()

    connection = FakeConnection()
    with patch.object(script.psycopg, "connect", return_value=connection):
        gaps = script.load_gaps("postgresql://example", ["hearings"])

    assert gaps == [("hearings", "109", 2890, 2891)]
    assert "actionable_missing_position_ranges" in connection.query
    assert "enumerated_row_count" not in connection.query
    assert connection.params == (["hearings"],)
