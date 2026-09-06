from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import zipfile

from psycopg import sql


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "compare_public_archive.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("compare_public_archive", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_count_csv_rows_handles_quoted_embedded_newline() -> None:
    script = _load_module()
    from io import StringIO

    assert script.count_csv_rows(StringIO('id,title\n1,"line one\nline two"\n2,plain\n')) == 2


def test_count_csv_rows_handles_large_document_fields() -> None:
    script = _load_module()
    from io import StringIO

    document = "x" * 200_000
    assert script.count_csv_rows(StringIO(f"id,text\n1,{document}\n")) == 1


def test_count_published_rows_checks_contract_headers(tmp_path: Path) -> None:
    script = _load_module()
    archive_dir = tmp_path
    with zipfile.ZipFile(archive_dir / "families.zip", "w") as archive:
        archive.writestr("things.csv", "id,title\n1,ok\n")
    document = {
        "families": [
            {
                "name": "things",
                "table": "things",
                "source_archive": "families.zip",
                "children": [],
                "columns": {"id": "text", "title": "text"},
                "source_file": "things.csv",
            }
        ]
    }
    assert script.count_published_rows(document, archive_dir) == {"things": 1}


def test_count_current_rows_composes_identifiers_and_keeps_missing_tables() -> None:
    script = _load_module()

    class Result:
        def __init__(self, rows):
            self.rows = rows

        def fetchall(self):
            return self.rows

        def fetchone(self):
            return self.rows[0] if self.rows else None

    class Connection:
        def __init__(self):
            self.queries = []

        def execute(self, query, params=None):
            self.queries.append(query)
            if isinstance(query, str):
                return Result([("things",)])
            return Result([(4,)])

    connection = Connection()
    counts = script.count_current_rows(connection, ["things", "missing"])

    assert counts == {"things": 4, "missing": None}
    assert any(isinstance(query, sql.Composed) for query in connection.queries)


def test_compare_counts_reports_delta_ratio_and_empty_current() -> None:
    script = _load_module()
    document = {
        "contract_version": 1,
        "contract_kind": "public_product_schema",
        "families": [
            {
                "name": "things",
                "table": "things",
                "children": [],
                "feature_status": "core-now",
                "columns": {"id": "text"},
            }
        ],
    }
    report = script.compare_counts(document, {"things": 4}, {"things": 0})

    row = report["tables"][0]
    assert row["current_count"] == 0
    assert row["published_count"] == 4
    assert row["delta"] == -4
    assert row["ratio"] == 0
    assert row["empty_current"] is True
    assert "empty_current" in row["flags"]
    assert report["empty_current_tables"] == ["things"]
    assert "| things | `things` | core-now | 0 | 4 | -4 | 0.000 | empty_current |" in script.render_markdown(report)
