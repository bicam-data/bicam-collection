from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import report_raw_version_retention as script


class FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]], rowcount: int = 0) -> None:
        self._rows = rows
        self.rowcount = rowcount

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class FakeConnection:
    """Replays canned results per statement and records everything executed."""

    def __init__(self, *, windows: list[list[tuple[Any, ...]]], resolver_table: bool = True, hash_index: bool = False) -> None:
        self.windows = list(windows)
        self.resolver_table = resolver_table
        self.hash_index = hash_index
        self.executed: list[tuple[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, sql: str, params: Any = None) -> FakeCursor:
        self.executed.append((sql, params))
        if sql in {"set statement_timeout = '60s'", "set default_transaction_read_only = on"}:
            return FakeCursor([])
        if sql == script.RESOLVER_TABLE_SQL:
            return FakeCursor([(self.resolver_table,)])
        if sql == script.RESPONSE_HASH_INDEX_SQL:
            return FakeCursor([(self.hash_index,)])
        if sql == script.ESTIMATED_VERSION_COUNT_SQL:
            return FakeCursor([(1_000_000,)])
        if sql == script.DELETE_SQL:
            return FakeCursor([], rowcount=len(params["version_ids"]))
        if "with window_versions as" in sql:
            return FakeCursor(self.windows.pop(0) if self.windows else [])
        raise AssertionError(f"unexpected statement: {sql[:80]}")

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _version_row(retention_class: str, payload_bytes: int = 100) -> tuple[str, int, str]:
    return (str(uuid.uuid4()), payload_bytes, retention_class)


def test_classification_sql_orders_classes_and_gates_optional_predicates() -> None:
    sql = script.classification_sql(resolver_table_present=True, response_hash_index_present=True)
    assert "int_congress_child_response_selections" in sql
    assert "other_current.response_hash = other.response_hash" in sql
    class_order = [name for name in script.RETAINED_CLASSES + script.PRUNE_CLASSES if name in sql]
    assert class_order == list(script.RETAINED_CLASSES + script.PRUNE_CLASSES)
    assert sql.index("'kept_current'") < sql.index("'kept_newest_k'") < sql.index("'prune_remainder'")

    gated = script.classification_sql(resolver_table_present=False, response_hash_index_present=False)
    assert "int_congress_child_response_selections" not in gated
    assert "other_current" not in gated
    assert "when false then 'kept_resolver_selected'" in gated
    assert "when false then 'prune_duplicate_payload'" in gated


def test_classification_sql_never_selects_current_versions_for_pruning() -> None:
    sql = script.classification_sql(resolver_table_present=True, response_hash_index_present=True)
    assert "when window_versions.is_current then 'kept_current'" in sql
    assert "delete" not in sql.lower()
    assert "using raw_responses as current_response" in script.DELETE_SQL
    assert "current_response.response_hash <> versions.response_hash" in script.DELETE_SQL
    assert "crawl_run_fetches" not in script.DELETE_SQL


def test_classify_window_binds_policy_and_keyset_parameters() -> None:
    connection = FakeConnection(windows=[[_version_row("kept_current")]])
    policy = script.RetentionPolicy(keep_versions=3, older_than_days=45, ledger_window_days=10)
    rows = script.classify_window(connection, "with window_versions as ...", policy, after_id="abc", batch_size=7)
    assert len(rows) == 1 and rows[0].retention_class == "kept_current"
    _sql, params = connection.executed[-1]
    assert params == {
        "after_id": "abc",
        "batch_size": 7,
        "keep_versions": 3,
        "older_than_days": 45,
        "ledger_window_days": 10,
    }


def test_dry_run_counts_classes_samples_candidates_and_never_deletes() -> None:
    window_one = [
        _version_row("kept_current", 10),
        _version_row("kept_newest_k", 20),
        _version_row("prune_duplicate_payload", 30),
        _version_row("prune_remainder", 40),
    ]
    window_two = [_version_row("kept_ledger_stale", 50), _version_row("prune_remainder", 60)]
    connection = FakeConnection(windows=[window_one, window_two])
    policy = script.RetentionPolicy(keep_versions=2, older_than_days=180, ledger_window_days=90)

    report = script.run_report(connection, policy, batch_size=4, limit=0, apply=False)

    assert report["mode"] == "dry-run"
    assert report["scan"]["versions_examined"] == 6
    assert report["scan"]["batches"] == 2
    assert report["scan"]["complete"] is True
    assert report["classes"]["prune_remainder"] == {"versions": 2, "payload_bytes": 100}
    assert report["classes"]["prune_duplicate_payload"] == {"versions": 1, "payload_bytes": 30}
    assert report["classes"]["kept_ledger_stale"] == {"versions": 1, "payload_bytes": 50}
    assert report["prunable"] == {
        "versions": 3,
        "payload_bytes": 130,
        "extrapolated_versions": 3,
        "extrapolated_payload_bytes": 130,
    }
    assert report["sample_candidate_ids"]["prune_remainder"] == [window_one[3][0], window_two[1][0]]
    assert report["sample_candidate_ids"]["prune_duplicate_payload"] == [window_one[2][0]]
    assert report["deleted_versions"] == 0
    assert all(sql != script.DELETE_SQL for sql, _params in connection.executed)


def test_limit_bounds_examined_rows_and_extrapolates() -> None:
    rows = [_version_row("kept_current")] * 3 + [_version_row("prune_remainder", 100)]
    connection = FakeConnection(windows=[rows[:2], rows[2:]])
    policy = script.RetentionPolicy(keep_versions=2, older_than_days=180, ledger_window_days=90)

    report = script.run_report(connection, policy, batch_size=2, limit=4, apply=False)

    assert report["scan"]["versions_examined"] == 4
    assert report["scan"]["complete"] is False
    assert [params["batch_size"] for sql, params in connection.executed if "with window_versions as" in sql] == [2, 2]
    assert report["prunable"]["extrapolated_versions"] == 250_000
    assert report["prunable"]["extrapolated_payload_bytes"] == 25_000_000


def test_apply_deletes_only_prunable_ids_in_bounded_batches() -> None:
    keep = _version_row("kept_ledger_recent")
    prune_a = _version_row("prune_remainder")
    prune_b = _version_row("prune_duplicate_payload")
    connection = FakeConnection(windows=[[keep, prune_a], [prune_b]])
    policy = script.RetentionPolicy(keep_versions=2, older_than_days=180, ledger_window_days=90)

    report = script.run_report(connection, policy, batch_size=2, limit=0, apply=True)

    deletes = [params for sql, params in connection.executed if sql == script.DELETE_SQL]
    assert deletes == [{"version_ids": [prune_a[0]]}, {"version_ids": [prune_b[0]]}]
    assert report["deleted_versions"] == 2
    assert report["mode"] == "apply"


def test_delete_batch_rolls_back_on_failure() -> None:
    class FailingConnection(FakeConnection):
        def execute(self, sql: str, params: Any = None) -> FakeCursor:
            raise psycopg.errors.ForeignKeyViolation("still referenced")

    connection = FailingConnection(windows=[])
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        script.delete_versions(connection, [str(uuid.uuid4())])
    assert connection.rollbacks == 1
    assert connection.commits == 0


def test_apply_without_confirm_refuses_before_connecting(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    def forbidden_connect(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("psycopg.connect must not be called")

    monkeypatch.setattr(script.psycopg, "connect", forbidden_connect)
    monkeypatch.setattr(script.Settings, "from_env", classmethod(lambda cls, root: (_ for _ in ()).throw(AssertionError("settings must not load"))))

    assert script.main(["--apply"]) == 2
    assert "refusing --apply without --confirm-delete" in capsys.readouterr().err


def test_parse_args_rejects_policies_that_could_drop_the_current_version() -> None:
    with pytest.raises(SystemExit):
        script.parse_args(["--keep-versions", "0"])
    with pytest.raises(SystemExit):
        script.parse_args(["--batch-size", "0"])
    args = script.parse_args([])
    assert (args.keep_versions, args.older_than_days, args.ledger_window_days) == (2, 180, 90)
    assert args.apply is False and args.confirm_delete is False


def test_dry_run_main_sets_read_only_session(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    connection = FakeConnection(windows=[[]])

    class ConnectionContext:
        def __enter__(self) -> FakeConnection:
            return connection

        def __exit__(self, *_args: Any) -> bool:
            return False

    monkeypatch.setattr(script.psycopg, "connect", lambda *_args, **_kwargs: ConnectionContext())
    monkeypatch.setattr(script.Settings, "from_env", classmethod(lambda cls, root: type("S", (), {"database_url": "postgresql://unused"})()))

    assert script.main(["--limit", "10"]) == 0
    session_statements = [sql for sql, _params in connection.executed[:2]]
    assert session_statements == ["set statement_timeout = '60s'", "set default_transaction_read_only = on"]
    assert not any(sql == script.DELETE_SQL for sql, _params in connection.executed)
    report = json.loads(capsys.readouterr().out)
    assert report["mode"] == "dry-run"
    assert report["scan"]["versions_examined"] == 0


@pytest.mark.skipif(
    not os.environ.get("BICAM_TEST_DATABASE_URL"),
    reason="requires BICAM_TEST_DATABASE_URL for an isolated Postgres test database",
)
def test_dry_run_and_guarded_apply_against_disposable_database() -> None:
    # Never fall back to the project .env or DATABASE_URL here; the fixture
    # writes and deletes rows, so it must only ever target the disposable DSN.
    database_url = os.environ["BICAM_TEST_DATABASE_URL"]
    suffix = uuid.uuid4().hex
    url = f"https://example.test/retention/{suffix}"
    run_id = str(uuid.uuid4())
    raw_id = str(uuid.uuid4())
    current_version_id = str(uuid.uuid4())
    old_version_ids = [str(uuid.uuid4()) for _ in range(3)]
    policy = script.RetentionPolicy(keep_versions=2, older_than_days=30, ledger_window_days=7)

    with psycopg.connect(database_url) as connection:
        try:
            connection.execute(
                "insert into crawl_runs (id, category, mode, status) values (%s, %s, 'full', 'completed')",
                (run_id, f"retention-test-{suffix}"),
            )
            connection.execute(
                """
                insert into raw_responses (id, endpoint_url, source, category, node_type, response, response_hash, latest_crawl_run_id)
                values (%s, %s, 'congress', %s, 'detail', '{"v": "current"}'::jsonb, 'hash-current', %s)
                """,
                (raw_id, url, f"retention-test-{suffix}", run_id),
            )
            versions = [(current_version_id, "hash-current", "1 day")] + [
                (version_id, f"hash-old-{index}", f"{100 + index} days")
                for index, version_id in enumerate(old_version_ids)
            ]
            for version_id, response_hash, age in versions:
                connection.execute(
                    f"""
                    insert into raw_response_versions (
                        id, raw_response_id, endpoint_url, source, category, node_type, response, response_hash,
                        first_seen_at, last_seen_at, first_crawl_run_id, latest_crawl_run_id)
                    values (%s, %s, %s, 'congress', %s, 'detail', %s::jsonb, %s,
                        now() - interval '{age}', now() - interval '{age}', %s, %s)
                    """,
                    (version_id, raw_id, url, f"retention-test-{suffix}", json.dumps({"v": response_hash}), response_hash, run_id, run_id),
                )
            # The oldest version stays referenced by the fetch ledger, which must
            # keep it (kept_ledger_stale) even though it is outside every window.
            connection.execute(
                """
                insert into crawl_run_fetches (id, crawl_run_id, endpoint_url, raw_response_id, raw_response_version_id,
                    node_type, follow_mode, http_status, outcome, fetched_at)
                values (%s, %s, %s, %s, %s, 'detail', 'full', 200, 'fetched', now() - interval '200 days')
                """,
                (str(uuid.uuid4()), run_id, url, raw_id, old_version_ids[2]),
            )
            connection.commit()
            connection.execute("set statement_timeout = '60s'")

            dry_run = script.run_report(connection, policy, batch_size=500, limit=0, apply=False)
            assert dry_run["scan"]["complete"] is True
            assert dry_run["deleted_versions"] == 0
            assert dry_run["classes"]["prune_remainder"]["versions"] >= 1
            assert old_version_ids[1] in dry_run["sample_candidate_ids"]["prune_remainder"]

            applied = script.run_report(connection, policy, batch_size=500, limit=0, apply=True)
            assert applied["deleted_versions"] >= 1
            remaining = {
                str(row[0])
                for row in connection.execute(
                    "select id from raw_response_versions where raw_response_id = %s", (raw_id,)
                ).fetchall()
            }
            assert current_version_id in remaining
            assert old_version_ids[0] in remaining  # newest superseded, within keep_versions=2
            assert old_version_ids[2] in remaining  # ledger-referenced
            assert old_version_ids[1] not in remaining
        finally:
            connection.rollback()
            connection.execute("delete from crawl_run_fetches where endpoint_url = %s", (url,))
            connection.execute("delete from raw_response_versions where endpoint_url = %s", (url,))
            connection.execute("delete from raw_responses where endpoint_url = %s", (url,))
            connection.execute("delete from crawl_runs where id = %s", (run_id,))
            connection.commit()
