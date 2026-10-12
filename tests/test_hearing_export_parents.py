import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")
spec = importlib.util.spec_from_file_location("hearing_capture", Path(__file__).parents[1] / "scripts/capture_hearing_export_parents.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def package(**kwargs):
    return {"packageId": "CHRG-119jhrg00123", "congress": "119", "title": "Joint testimony",
            "lastModified": "2026-09-09T23:09:59Z", **kwargs}


def test_joint_identity_and_exact_source_types():
    row = module.project_parent("jhrg123-119", package(pages="12", session="1"))
    assert row["chamber"] == "joint"
    assert row["pages"] == 12 and row["session"] == 1
    assert row["hearing_jacketnumber"] == "123"
    assert row["hearing_number"] is None


@pytest.mark.parametrize("expected,raw", [("hhrg123-119", package()),
    ("jhrg123-118", package()), ("jhrg123-119", package(congress="118")),
    ("jhrg123-119", package(lastModified=None))])
def test_identity_or_date_contradiction_rejected(expected, raw):
    with pytest.raises(ValueError):
        module.project_parent(expected, raw)


def test_version_selection_requires_unambiguous_observed_events():
    earlier = (None,) * 7 + ("2026-09-01", None)
    later = (None,) * 7 + ("2026-09-09", None)
    assert module.choose_observed_version([earlier, later]) == later
    assert module.choose_observed_version([earlier]) == earlier
    for rows in ([earlier, earlier], [earlier, (None,) * 9]):
        with pytest.raises(ValueError):
            module.choose_observed_version(rows)
