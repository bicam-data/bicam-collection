from __future__ import annotations

import importlib.util
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "resolve_current_congress.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("resolve_current_congress", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_congress_accepts_only_positive_integers() -> None:
    script = _load_module()
    assert script.parse_congress("119") == 119
    assert script.parse_congress(120) == 120
    assert script.parse_congress("0") is None
    assert script.parse_congress("119th") is None
    assert script.parse_congress(None) is None


def test_override_is_used_without_opening_database() -> None:
    script = _load_module()
    assert script.resolve_current_congress("postgresql://unused", override="120") == 120


def test_query_is_scoped_to_congress_detail_archive() -> None:
    script = _load_module()
    assert "category = 'congresses'" in script.CURRENT_CONGRESS_SQL
    assert "node_type = 'congress_detail'" in script.CURRENT_CONGRESS_SQL
