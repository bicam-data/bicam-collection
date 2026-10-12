from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


PATH = Path(__file__).resolve().parents[1] / "scripts/replay_raw_research_product.py"
SPEC = importlib.util.spec_from_file_location("replay_raw_research_product", PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_frozen_raw_payloads_and_ids_are_deterministic():
    first, second = MODULE.frozen_rows(), MODULE.frozen_rows()
    assert first == second
    assert len(first) == 42
    assert len({row["version_id"] for row in first}) == len(first)
    raw_ids = {row["raw_id"] for row in first}
    assert all(row["parent_id"] is None or row["parent_id"] in raw_ids for row in first)
    assert all(row["observed_at"].startswith("2026-09-01T") for row in first)


def test_raw_replay_refuses_same_database_before_writes(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("must not connect")
    monkeypatch.setattr(MODULE.psycopg, "connect", forbidden)
    with pytest.raises(ValueError, match="different"):
        MODULE.run(["same", "same"], tmp_path / "output")
    assert not (tmp_path / "output").exists()


def test_raw_replay_refuses_production_database(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("must not connect")
    monkeypatch.setattr(MODULE.psycopg, "connect", forbidden)
    with pytest.raises(ValueError, match="bicam_replay_"):
        MODULE.require_empty("postgresql://admin@127.0.0.1:5433/bicam_collection")
