from pathlib import Path
import importlib.util
import os

import pytest


_MODULE_SPEC = importlib.util.spec_from_file_location(
    "validate_public_schema",
    Path(__file__).resolve().parents[1] / "scripts" / "validate_public_schema.py",
)
assert _MODULE_SPEC and _MODULE_SPEC.loader
_MODULE = importlib.util.module_from_spec(_MODULE_SPEC)
_MODULE_SPEC.loader.exec_module(_MODULE)
validate_public_schema = _MODULE.validate_public_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(
    not os.environ.get("BICAM_PUBLISHED_ARCHIVE_DIR"),
    reason="requires the published 2025 BICAM CSV archives",
)
def test_public_schema_matches_published_2025_archives() -> None:
    families, tables = validate_public_schema(
        PROJECT_ROOT / "contracts" / "bicam_public_schema.yaml",
        Path(os.environ["BICAM_PUBLISHED_ARCHIVE_DIR"]),
    )
    assert families == 11
    assert tables == 91


def test_deferred_features_are_explicit() -> None:
    import yaml

    contract = yaml.safe_load(
        (PROJECT_ROOT / "contracts" / "bicam_public_schema.yaml").read_text(encoding="utf-8")
    )
    deferred = {
        table["table"]
        for family in contract["families"]
        for table in [family, *family.get("children", [])]
        if table["feature_status"] == "deferred-external-crosswalk"
    }
    assert deferred == {
        "bills_lobbied",
    }


def test_nominee_contract_declares_grouping_not_unique_key() -> None:
    import yaml

    contract = yaml.safe_load(
        (PROJECT_ROOT / "contracts" / "bicam_public_schema.yaml").read_text(encoding="utf-8")
    )
    nominee = next(
        child
        for family in contract["families"]
        for child in family.get("children", [])
        if child["table"] == "nominations_nominees"
    )
    assert "primary_key" not in nominee
    assert nominee["key_semantics"] == "grouping"
    assert nominee["grouping_key"] == ["nomination_id", "ordinal"]
    assert nominee["export_order"][:2] == nominee["grouping_key"]
