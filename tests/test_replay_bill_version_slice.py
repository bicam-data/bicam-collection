from __future__ import annotations

import gzip
import importlib.util
import os
from pathlib import Path
from decimal import Decimal
import json
import sys

import pytest


PATH = Path(__file__).resolve().parents[1] / "scripts/replay_bill_version_slice.py"
SPEC = importlib.util.spec_from_file_location("replay_bill_version_slice", PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


@pytest.mark.parametrize("url", [
    None,
    "postgresql://user@127.0.0.1:5433/bicam_collection",
    "postgresql://user@remote.example:5432/bicam_replay_test",
    "postgresql://user@127.0.0.1:5432/bicam_replay_test?hostaddr=192.0.2.1",
    "postgresql://user@127.0.0.1:5432/bicam_replay_test?service=production",
    "postgresql://user@127.0.0.1/bicam_replay_test",
    "postgresql://127.0.0.1:5432/bicam_replay_test",
])
def test_unsafe_database_rejected_before_connection(url, monkeypatch, tmp_path: Path) -> None:
    def forbidden(*args, **kwargs):
        raise AssertionError("must reject unsafe database before connecting")
    monkeypatch.setattr(MODULE.psycopg, "connect", forbidden)
    with pytest.raises(ValueError):
        MODULE.run_replay(url, tmp_path / "output")
    assert not (tmp_path / "output").exists()


def test_production_environment_is_not_a_test_fallback(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://production")
    with pytest.raises(ValueError, match="no production DSN fallback"):
        MODULE.validate_test_url(None)


@pytest.mark.parametrize("variable", ["PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE"])
def test_ambient_connection_redirect_rejected(monkeypatch, variable) -> None:
    monkeypatch.setenv(variable, "unexpected")
    with pytest.raises(ValueError, match="ambient"):
        MODULE.validate_test_url("postgresql://user@127.0.0.1:5432/bicam_replay_test")


def test_canonical_json_and_gzip_are_deterministic() -> None:
    first = MODULE.canonical_bytes({"text": "café", "number": None})
    second = MODULE.canonical_bytes({"number": None, "text": "café"})
    assert first == second
    zipped = MODULE.deterministic_gzip(first)
    assert zipped == MODULE.deterministic_gzip(second)
    assert zipped[4:8] == b"\0\0\0\0"
    assert gzip.decompress(zipped) == first


def test_archived_models_are_exact_production_sql(tmp_path: Path) -> None:
    project = MODULE._archive_project(tmp_path)
    for name in MODULE.PUBLIC_TABLES:
        assert (project / f"models/{name}.sql").read_bytes() == (MODULE.ROOT / f"dbt_project/models/marts/bicam/{name}.sql").read_bytes()
    assert not (project / "profiles.yml").exists()
    assert not list(tmp_path.rglob(".env"))


def test_cli_can_print_decimal_diagnostics(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(sys, "argv", ["replay", "--output-dir", str(tmp_path / "output")])
    monkeypatch.setattr(MODULE, "run_replay", lambda *args: {"diagnostics": {"confidence": Decimal("1.0")}, "files": {}})
    assert MODULE.main() == 0
    assert json.loads(capsys.readouterr().out) == {"diagnostics": {"confidence": "1.0"}}


@pytest.mark.skipif(os.environ.get("BICAM_RUN_VERSION_REPLAY") != "1", reason="requires explicitly enabled empty disposable replay database")
def test_real_dbt_replay_and_source_traces(tmp_path: Path) -> None:
    url = os.environ["BICAM_TEST_DATABASE_URL"]
    report = MODULE.run_replay(url, tmp_path / "replay")
    assert report["replay_passed"] is True
    assert report["research_release_ready"] is False
    assert report["builds"][0] == report["builds"][1]
    assert report["builds"][0]["bills_versions_traces"]["rows"] == 7
    assert report["mixed_raw_version_rows"] == 0
    assert report["unresolved_raw_version_rows"] == 3
    assert report["builds"][0]["bills_version_links_traces"]["rows"] == 13
    # Fail closed if a caller attempts to rerun against the now populated DB.
    with pytest.raises(ValueError, match="empty disposable database"):
        MODULE.run_replay(url, tmp_path / "must-not-exist")
    assert not (tmp_path / "must-not-exist").exists()
