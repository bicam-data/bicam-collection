from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts/backfill_fetch_version_links.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("backfill_fetch_version_links", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_temporal_resolution_is_opt_in() -> None:
    script = _load_script()
    assert script.parse_args([]).resolve_temporal is False
    assert script.parse_args(["--resolve-temporal", "--skip-count"]).resolve_temporal is True


def test_endpoint_resolution_is_opt_in() -> None:
    script = _load_script()
    assert script.parse_args([]).resolve_endpoint is False
    assert script.parse_args(["--resolve-endpoint", "--skip-count"]).resolve_endpoint is True
