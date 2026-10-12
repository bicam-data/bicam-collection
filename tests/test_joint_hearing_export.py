import importlib.util
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyarrow")
scripts = Path(__file__).parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("joint_hearing_export", scripts / "repair_joint_hearing_export.py")
module = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
try:
    spec.loader.exec_module(module)
finally:
    sys.path.pop(0)


def relation(name, keys, fields=None):
    return {"name": name, "primary_key": keys, "key_semantics": "unique",
            "foreign_keys": fields if fields is not None else {"hearing_id": "hearings.hearing_id"}}


def test_meeting_edges_union_without_duplicating_joint_event():
    source = [{"meeting_id": "336804", "hearing_id": old} for old in sorted(module.ALIASES)]
    source.append({"meeting_id": "other", "hearing_id": "hhrg1-119"})
    rows, affected, reduction = module.transform(relation("committee_meetings_associated_hearings", ["meeting_id", "hearing_id"]), source, {})
    assert reduction == 1 and len(affected) == 2
    assert rows == [{"meeting_id": "other", "hearing_id": "hhrg1-119"}, {"meeting_id": "336804", "hearing_id": module.CANONICAL}]
    assert source[0]["hearing_id"] in module.ALIASES  # original observations preserved


def test_all_declared_hearing_fk_columns_are_transformed():
    spec = relation("future_child", ["id"], {"linked_hearing": "hearings.hearing_id"})
    rows, _, _ = module.transform(spec, [{"id": "x", "linked_hearing": "shrg60081-119"}], {})
    assert rows[0]["linked_hearing"] == module.CANONICAL


def test_conflicting_committee_payload_is_not_silently_discarded():
    source = [{"hearing_id": old, "committee_code": "sssb00", "committee_name": name}
              for old, name in zip(sorted(module.ALIASES), ["One name", "Different name"], strict=True)]
    with pytest.raises(ValueError, match="conflicting"):
        module.transform(relation("hearings_committees", ["hearing_id", "committee_code"]), source, {})


def test_rendition_prefers_observed_url_without_fabricating_joint_path():
    source = [{"hearing_id": old, "raw_text": None, "pdf": f"https://example.test/{old}.pdf"}
              for old in sorted(module.ALIASES)]
    rows, affected, reduction = module.transform(relation("hearings_texts", ["hearing_id"]), source, {})
    assert len(rows) == 1 and reduction == 1 and len(affected) == 2
    assert rows[0]["hearing_id"] == module.CANONICAL
    assert rows[0]["pdf"] == "https://example.test/hhrg60081-119.pdf"


def test_source_roots_collapse_into_one_joint_root_with_retained_series_citation():
    root = {"hearing_id": module.CANONICAL, "chamber": "joint", "citation": "H.Hrg.119-8"}
    source = [{"hearing_id": old, "chamber": chamber} for old, chamber in zip(sorted(module.ALIASES), ["house", "senate"], strict=True)]
    rows, original, reduction = module.transform(relation("hearings", ["hearing_id"], {}), source, root)
    assert rows == [root] and original == source and reduction == 1


def test_evidence_path_and_hash_must_match(tmp_path):
    path = tmp_path / "evidence.json"
    path.write_text("{}")
    assert module.checked_relative(tmp_path, path.name, module.sha(path)) == path
    with pytest.raises(ValueError, match="path/hash"):
        module.checked_relative(tmp_path, path.name, "wrong")
