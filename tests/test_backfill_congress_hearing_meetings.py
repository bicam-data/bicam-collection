from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "backfill_congress_hearing_meetings.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_congress_hearing_meetings", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_loader_uses_hearing_associated_meeting_and_defers_recent_404s() -> None:
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
            limit=25,
            retry_404_after_hours=72.0,
        )

    assert "associatedMeeting" in connection.query
    assert "committee-meetings" in connection.query
    assert "from failed_requests as prior_failure" in connection.query
    assert "from crawl_run_fetches as prior" not in connection.query
    assert "prior_failure.http_status = 404" in connection.query
    assert connection.params == (72.0, 25)


def test_loader_rejects_nonpositive_404_ttl() -> None:
    script = _load_module()

    with patch.object(script.psycopg, "connect"):
        try:
            script.load_missing(
                database_url="postgresql://example",
                limit=25,
                retry_404_after_hours=0,
            )
        except ValueError as exc:
            assert "retry_404_after_hours" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected a positive retry_404_after_hours validation error")
