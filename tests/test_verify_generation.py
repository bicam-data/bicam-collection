from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil

import pytest


PATH = Path(__file__).resolve().parents[1] / "scripts/verify_replay_generation.py"
SPEC = importlib.util.spec_from_file_location("verify_replay_generation", PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_normalization_preserves_null_boolean_and_timestamp():
    assert MODULE.normalized(None, "text") is None
    assert MODULE.normalized("", "text") == ""
    assert MODULE.normalized(True, "boolean") == MODULE.normalized("t", "boolean")
    assert MODULE.normalized("2026-09-01T00:00:00Z", "timestamptz") == MODULE.normalized("2026-08-31 20:00:00-04", "timestamptz")
    with pytest.raises(ValueError):
        MODULE.normalized("2026-09-01", "timestamptz")
    with pytest.raises(ValueError):
        MODULE.normalized(1, "boolean")


@pytest.mark.parametrize("name", ["../outside", "/etc/passwd", "./inside", ""])
def test_generation_paths_are_bounded(tmp_path, name):
    with pytest.raises(ValueError):
        MODULE.checked_path(tmp_path, name)


def test_generation_symlink_rejected(tmp_path):
    (tmp_path / "file").write_text("fixture")
    (tmp_path / "link").symlink_to(tmp_path / "file")
    with pytest.raises(ValueError, match="symlinked"):
        MODULE.checked_path(tmp_path, "link")


@pytest.mark.skipif(not os.environ.get("BICAM_REPLAY_BUILD_DIR"), reason="requires a generated disposable raw-replay artifact")
@pytest.mark.parametrize("defect", ["source", "candidate", "failed_run", "wrong_row", "unknown_raw_version"])
def test_real_replay_rejects_tampering(tmp_path, defect):
    source = Path(os.environ["BICAM_REPLAY_BUILD_DIR"])
    archive = tmp_path / "archive"
    shutil.copytree(source.parent, archive)
    build = archive / source.name
    assert MODULE.verify(build)["valid"]
    binding_path = build / "generation-binding.json"
    binding = json.loads(binding_path.read_text())
    if defect == "source":
        (archive / "project/dbt_project.yml").write_text("changed")
    elif defect == "candidate":
        candidate = next((build / "candidate").glob("*/bills.csv.gz"))
        with candidate.open("ab") as stream:
            stream.write(b"tampered")
    elif defect == "failed_run":
        path = build / "run/run_results.json"
        data = json.loads(path.read_text())
        data["results"][0]["status"] = "error"
        path.write_text(json.dumps(data))
        binding["dbt_run_results_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    else:
        path = build / "bill-row-lineage.json"
        data = json.loads(path.read_text())
        if defect == "wrong_row":
            data[0]["public_row"]["bill_id"] = "hr999999-118"
        else:
            data[0]["evidence"][0]["raw_version_id"] = "unknown"
        path.write_text(json.dumps(data))
        binding["bill_row_lineage_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    binding_path.write_text(json.dumps(binding))
    with pytest.raises(ValueError):
        MODULE.verify(build)
