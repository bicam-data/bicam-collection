from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "backfill_congressional_record_index.py"
SPEC = importlib.util.spec_from_file_location("backfill_congressional_record_index", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)


def test_page_offsets_cover_nonstandard_legacy_count() -> None:
    assert module.page_offsets(5856, 250) == list(range(0, 5856, 250))
    assert module.page_offsets(0, 250) == [0]


def test_page_offsets_reject_invalid_values() -> None:
    with pytest.raises(ValueError):
        module.page_offsets(1, 0)
    with pytest.raises(ValueError):
        module.page_offsets(-1, 250)
