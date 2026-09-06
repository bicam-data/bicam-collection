from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "retry_failed_fetches.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("retry_failed_fetches", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_no_descendants_retry_is_explicit_and_bounded() -> None:
    script = _load_module()
    args = script.parse_args(["--no-descendants"])

    assert args.categories == list(script.TARGET_CONGRESS_CATEGORIES)
    assert args.no_descendants is True
    assert args.max_pages == 1000
    assert args.min_age_minutes == 15.0
    assert args.include_source_500 is False
    assert args.only_source_500 is False
    assert args.endpoint is None
    assert args.node_types is None


def test_source_500_retries_require_explicit_opt_in() -> None:
    script = _load_module()
    args = script.parse_args(["--include-source-500", "--min-age-minutes", "30"])

    assert args.include_source_500 is True
    assert args.min_age_minutes == 30.0


def test_retry_categories_can_be_explicitly_overridden() -> None:
    script = _load_module()

    args = script.parse_args(["--categories", "govinfo-bills", "--dry-run"])

    assert args.categories == ["govinfo-bills"]


def test_source_500_only_retry_is_explicitly_narrow() -> None:
    script = _load_module()
    args = script.parse_args(["--include-source-500", "--only-source-500"])

    assert args.include_source_500 is True
    assert args.only_source_500 is True


def test_node_type_filter_is_explicitly_preserved() -> None:
    script = _load_module()

    args = script.parse_args(["--node-types", "amendment_detail", "bill_detail", "--dry-run"])

    assert args.node_types == ["amendment_detail", "bill_detail"]


def test_without_descendants_does_not_mutate_the_loaded_spec() -> None:
    script = _load_module()
    original = load_spec(PROJECT_ROOT / "specs" / "amendments.yaml")
    assert any(node.child_edges or node.linked_item_edges for node in original.nodes.values())

    retry_spec = script._without_descendants(original)

    assert retry_spec is not original
    assert all(not node.child_edges and not node.linked_item_edges for node in retry_spec.nodes.values())
    assert any(node.child_edges or node.linked_item_edges for node in original.nodes.values())


def test_seed_path_can_replace_a_failed_list_page_size() -> None:
    script = _load_module()

    path = script._seed_path(
        "https://api.congress.gov/v3/hearing/109?offset=2750&limit=250&format=json",
        "congress",
        page_size=100,
    )

    assert path == "/hearing/109?offset=2750&limit=100&format=json"


def test_source_500_retry_size_is_bounded_without_broadening_small_pages() -> None:
    script = _load_module()

    large_page = script.FailedFetch(
        category="bills",
        endpoint_url="https://api.congress.gov/v3/bill/119?format=json&limit=250&offset=250",
        node_type="bill_list",
        failed_at=None,
        http_status=500,
    )
    malformed_child_page = script.FailedFetch(
        category="bills",
        endpoint_url="https://api.congress.gov/v3/bill/117/hr/1/cosponsors?format=json&limit=1&offset=70",
        node_type="bill_cosponsors",
        failed_at=None,
        http_status=500,
    )
    transient = script.FailedFetch(
        category="bills",
        endpoint_url="https://api.congress.gov/v3/bill/119?format=json&limit=250&offset=250",
        node_type="bill_list",
        failed_at=None,
        http_status=503,
    )

    assert script._retry_page_size(large_page, 250) == 10
    assert script._retry_page_size(malformed_child_page, 250) == 1
    assert script._retry_page_size(transient, 250) == 250


def test_retry_query_filters_status_after_selecting_latest_failure(monkeypatch) -> None:
    """A later terminal 404 must suppress an older transient retry seed."""

    script = _load_module()
    captured: dict[str, object] = {}

    class FakeResult:
        def fetchall(self):
            return []

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params):
            captured["query"] = query
            captured["params"] = params
            return FakeResult()

    monkeypatch.setattr(script.psycopg, "connect", lambda *_args, **_kwargs: FakeConnection())

    assert script.load_failed_fetches(
        database_url="postgresql://unused",
        limit=10,
        categories=["bills"],
        min_age_minutes=15,
    ) == []

    query = captured["query"]
    assert isinstance(query, str)
    assert "latest_failed_requests as" in query
    assert "latest_ledger_failures as" not in query
    assert "known_source_errors as" in query
    assert "successful_list_partitions as materialized" in query
    assert "enumerated_item_count" in query
    assert "responses.response ? 'pagination'" in query
    assert "'/(house|senate|joint|nochamber)$'" in query
    # The query is executed through psycopg, so literal LIKE wildcards must
    # remain doubled until the driver interpolates its placeholders.
    assert "source_error=%%" in query
    assert query.index("latest_failed_requests as") < query.index("failed.http_status = any")


def test_source_500_only_query_uses_literal_500_filters(monkeypatch) -> None:
    script = _load_module()
    captured: dict[str, object] = {}

    class FakeResult:
        def fetchall(self):
            return []

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params):
            captured["query"] = query
            captured["params"] = params
            return FakeResult()

    monkeypatch.setattr(script.psycopg, "connect", lambda *_args, **_kwargs: FakeConnection())
    script.load_failed_fetches(
        database_url="postgresql://unused",
        limit=10,
        categories=["bills"],
        min_age_minutes=0,
        include_source_500=True,
        only_source_500=True,
    )

    query = captured["query"]
    assert isinstance(query, str)
    assert "failed.http_status = 500" in query
    assert "http_status = any(%s)" not in query
    # Explicit source-500 opt-in must actually bypass the ordinary
    # known-source-error suppression clause.
    assert "from known_source_errors as known" not in query


def test_endpoint_filter_limits_compact_failure_ledger(monkeypatch) -> None:
    script = _load_module()
    captured: dict[str, object] = {}

    class FakeResult:
        def fetchall(self):
            return []

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params):
            captured["query"] = query
            captured["params"] = params
            return FakeResult()

    monkeypatch.setattr(script.psycopg, "connect", lambda *_args, **_kwargs: FakeConnection())
    endpoint = "https://api.congress.gov/v3/bill/91?format=json&limit=250&offset=250"
    script.load_failed_fetches(
        database_url="postgresql://unused",
        limit=1,
        categories=["bills"],
        endpoint_url=endpoint,
        min_age_minutes=0,
        include_source_500=True,
        only_source_500=True,
    )

    query = captured["query"]
    assert isinstance(query, str)
    assert query.count("endpoint_url = %s") == 1
    assert captured["params"] == [0, ["bills"], endpoint, 1]


def test_retry_run_summary_exposes_rate_limits(monkeypatch) -> None:
    script = _load_module()
    captured: dict[str, object] = {}

    class FakeResult:
        def fetchone(self):
            return (3, 2)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params):
            captured["query"] = query
            captured["params"] = params
            return FakeResult()

    monkeypatch.setattr(script.psycopg, "connect", lambda *_args, **_kwargs: FakeConnection())

    assert script.summarize_retry_run(database_url="postgresql://unused", crawl_run_id="run") == (3, 2)
    assert "count(*) filter (where http_status = 429)" in captured["query"]
