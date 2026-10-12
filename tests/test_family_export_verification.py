from collections import Counter
import importlib.util
import json
from pathlib import Path
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
scripts = Path(__file__).parents[1] / "scripts"
sys.path.insert(0, str(scripts))
try:
    spec = importlib.util.spec_from_file_location("family_delta", scripts / "verify_family_repair_delta.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
finally:
    sys.path.pop(0)


def test_row_fidelity_distinguishes_null_empty_case_and_duplicate_multiplicity():
    hashes = [module.row_digest({"text": v}) for v in (None, "", "TEXT", "text")]
    assert len(set(hashes)) == 4
    assert Counter(hashes) != Counter([*hashes, hashes[0]])


def test_exact_delta_rejects_unexpected_additions(tmp_path, monkeypatch):
    before, after = tmp_path / "before", tmp_path / "after"
    before.mkdir()
    after.mkdir()
    columns = [{"name": "bioguide_id", "postgres_type": "text"}]
    base = {"source_manifest_sha256": "frozen", "tables": [
        {"name": "members", "file": "members.parquet", "columns": columns, "row_count": 1}]}
    (before / "manifest.json").write_text(json.dumps(base))
    common = {"candidate_manifest_sha256": module.sha(before / "manifest.json")}
    amendment_file, hearing_file = tmp_path / "amendments.json", tmp_path / "hearings.json"
    amendment_file.write_text(json.dumps({**common, "root_replacements": [], "child_additions": {}}))
    hearing_file.write_text(json.dumps({**common, "additions": {}}))
    current = {**base, "amendment_supplement_sha256": module.sha(amendment_file),
               "child_supplement_sha256": module.sha(hearing_file)}
    (after / "manifest.json").write_text(json.dumps(current))
    monkeypatch.setattr(module, "validate", lambda p: json.loads((p / "manifest.json").read_text()))
    pq.write_table(pa.table({"bioguide_id": ["A000001"]}), before / "members.parquet")
    pq.write_table(pa.table({"bioguide_id": ["A000001"]}), after / "members.parquet")
    assert module.verify(before, after, amendment_file, hearing_file)["unexpected_changes"] == 0
    pq.write_table(pa.table({"bioguide_id": ["A000001", "A000001"]}), after / "members.parquet")
    with pytest.raises(ValueError, match="unexpected changed"):
        module.verify(before, after, amendment_file, hearing_file)
