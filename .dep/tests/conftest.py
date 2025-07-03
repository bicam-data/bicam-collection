from contextlib import asynccontextmanager
from types import SimpleNamespace

import asyncpg
import pytest


##############################################################################
# Mini in-memory Postgres replacement
##############################################################################
class _FakeConn(SimpleNamespace):
    """Very small subset of asyncpg.Connection used in the tests."""

    def __init__(self):
        super().__init__()
        # two tables that the tests read/write
        self._tables = {
            "bicam_staging_govinfo.bills": {},
            "bicam_metadata.govinfo_last_processed_dates": {},
        }

    # ---------- DDL helpers (no-ops for this fake) ---------- #
    async def fetchval(self, query, *args):
        # SELECT … FROM … WHERE …
        if "FROM bicam_metadata.govinfo_last_processed_dates" in query:
            data_type = args[0]
            return self._tables["bicam_metadata.govinfo_last_processed_dates"].get(
                data_type
            )
        return None

    async def fetchrow(self, query, *args):
        # SELECT … FROM bicam_staging_govinfo.bills WHERE bill_id = $1
        if "FROM bicam_staging_govinfo.bills" in query:
            bill_id = args[0]
            row = self._tables["bicam_staging_govinfo.bills"].get(bill_id)
            return row
        return None

    async def execute(self, query, *args):
        # INSERTs that appear in the tests
        if "INSERT INTO bicam_staging_govinfo.bills" in query:
            bill_id, title, last_modified = args
            self._tables["bicam_staging_govinfo.bills"][bill_id] = {
                "bill_id": bill_id,
                "title": title,
                "last_modified": last_modified,
            }

        elif "INSERT INTO bicam_metadata.govinfo_last_processed_dates" in query:
            data_type, last_processed = args
            self._tables["bicam_metadata.govinfo_last_processed_dates"][data_type] = (
                last_processed
            )

    # ---------- clean-up ---------- #
    async def close(self):
        pass


class _FakePool:
    """Mimics asyncpg.Pool API used in the test suite."""

    def __init__(self):
        self._conn = _FakeConn()

    @asynccontextmanager
    async def acquire(self):
        yield self._conn

    async def close(self):
        pass


##############################################################################
# Pytest-wide patch
##############################################################################
@pytest.fixture(scope="session", autouse=True)
def patch_asyncpg():
    """Replace asyncpg.create_pool/connect with in-memory fakes everywhere.

    Can't depend on the function-scoped ``monkeypatch`` fixture from a session-
    scoped fixture, so use ``pytest.MonkeyPatch`` directly.
    """

    mp = pytest.MonkeyPatch()

    async def _fake_create_pool(*_args, **_kwargs):
        return _FakePool()

    async def _fake_connect(*_args, **_kwargs):
        return _FakeConn()

    mp.setattr(asyncpg, "create_pool", _fake_create_pool, raising=True)
    mp.setattr(asyncpg, "connect", _fake_connect, raising=True)

    yield

    mp.undo()
