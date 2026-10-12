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
