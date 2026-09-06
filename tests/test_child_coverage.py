from pathlib import Path
import importlib.util


_MODULE_SPEC = importlib.util.spec_from_file_location(
    "check_child_coverage",
    Path(__file__).resolve().parents[1] / "scripts" / "check_child_coverage.py",
)
assert _MODULE_SPEC and _MODULE_SPEC.loader
_MODULE = importlib.util.module_from_spec(_MODULE_SPEC)
_MODULE_SPEC.loader.exec_module(_MODULE)
_edges = _MODULE._edges
_configure_audit_connection = _MODULE._configure_audit_connection
_coverage_totals = _MODULE._coverage_totals


def test_child_edges_exclude_roots_and_include_treaty_fallback() -> None:
    edges = _edges(Path(__file__).resolve().parents[1])

    assert edges
    assert all(not parent.endswith("_list") for _, parent, *_ in edges)
    assert (
        "treaties",
        "treaty_detail",
        "treaty_committees",
        "$.treaty[*].committees.url",
        "committees",
        "congress_treaty_committees",
    ) in edges


def test_audit_connection_disables_parallel_workers() -> None:
    calls: list[str] = []

    class Connection:
        def execute(self, statement: str) -> None:
            calls.append(statement)

    _configure_audit_connection(Connection())

    assert "set statement_timeout='120s'" in calls
    assert "set lock_timeout='15s'" in calls
    assert "set max_parallel_workers_per_gather=0" in calls


def test_item_level_pagination_gaps_contribute_to_missing_total() -> None:
    totals = _coverage_totals(
        [
            {"expected": 10, "fetched": 9, "source_gaps": 0, "missing": 1},
            {
                "coverage_level": "item",
                "expected": 100,
                "fetched": 70,
                "source_gaps": 5,
                "missing": 25,
            },
        ]
    )

    assert totals["endpoint_missing"] == 1
    assert totals["item_missing"] == 25
    assert totals["missing"] == 26
