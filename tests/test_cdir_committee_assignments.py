from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "capture_cdir_committee_assignments.py"
SPEC = importlib.util.spec_from_file_location("capture_cdir_committee_assignments", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_parse_compact_directory_rows_and_roles() -> None:
    text = """
ASSIGNMENTS OF REPRESENTATIVES, DELEGATES, AND RESIDENT COMMISSIONER TO COMMITTEES
Member Committees (Standing, Select, and Joint)
Alma S. Adams (NC-12) ................. Agriculture. Education and the Workforce. \
Robert B. Aderholt (AL-04) .......... Appropriations.
Jodey C. Arrington (TX-19) .......... Budget CHAIR. Ways and Means.
HOUSE LEADERSHIP
"""
    rows = MODULE.parse_assignments(text)
    assert [(row["member_name"], row["committee_name"]) for row in rows] == [
        ("Alma S. Adams (NC-12)", "Agriculture"),
        ("Alma S. Adams (NC-12)", "Education and the Workforce"),
        ("Robert B. Aderholt (AL-04)", "Appropriations"),
        ("Jodey C. Arrington (TX-19)", "Budget"),
        ("Jodey C. Arrington (TX-19)", "Ways and Means"),
    ]
    assert rows[3]["role"] == "chair"


def test_parse_dotted_directory_rows() -> None:
    text = """
ASSIGNMENTS OF SENATORS TO COMMITTEES
Tammy Baldwin ....................... Appropriations. Commerce, Science, and Transportation.
John Barrasso ........................... Energy and Natural Resources.
"""
    rows = MODULE.parse_assignments(text)
    assert len(rows) == 3
    assert {row["member_name"] for row in rows} == {"Tammy Baldwin", "John Barrasso"}
