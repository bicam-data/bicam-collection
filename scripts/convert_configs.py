#!/usr/bin/env python3
"""convert_configs.py – one-off helper to migrate *legacy* Congressional
`config.yaml` files to the new three-section structure expected by
`bicam_collection.data_types.config_manager`.

USAGE
-----
    python scripts/convert_configs.py  [--dry-run]

When *--dry-run* is supplied the script prints the would-be changes instead of
rewriting files.

The converter **only** touches YAML files under
`src/bicam_collection/data_types/congressional/**/config.yaml`.  GovInfo (and
any other) configs are ignored for now.

Conversion rules
----------------
* Keys moved to `api` section
    - expected_key       → api.expected_key
    - outer_api_field    → api.outer_field
    - page_size          → api.page_size
    - retry_attempts     → api.retry_attempts

* Keys moved to `schema` section
    - table_name, is_main, create_raw, id_fields, fields, nested_fields
    - related_fields     → schema.related_tables

* Keys moved to `processing` section
    - checkpoint_frequency
    - batch_size         → processing.batch_size   (was ambiguous; we treat it as
                                             pipeline batch size)

Any unknown / unmapped keys are left in place under the *root* for manual
review (the new loader will simply ignore them).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Union
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CFG_GLOB = PROJECT_ROOT / "src/bicam_collection/data_types/congressional"

LEGACY_TO_API = {
    "expected_key": "expected_key",
    "outer_api_field": "outer_field",
    "page_size": "page_size",
    "retry_attempts": "retry_attempts",
}

LEGACY_TO_SCHEMA = {
    "table_name": "table_name",
    "is_main": "is_main",
    "create_raw": "create_raw",
    "id_fields": "id_fields",
    "fields": "fields",
    "nested_fields": "nested_fields",
    # special case handled separately: related_fields → related_tables
}

LEGACY_TO_PROCESSING = {
    "checkpoint_frequency": "checkpoint_frequency",
    "batch_size": "batch_size",  # assumed to be ETL batch size
}

###############################################################################


def convert_single_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return a *new-format* mapping for one legacy data-type config."""
    new_cfg: Dict[str, Any] = {
        "name": cfg.get("name", cfg.get("table_name", "")),
        "api": {},
        "schema": {},
        "processing": {},
    }

    # --- direct moves --------------------------------------------------------
    for old_key, new_key in LEGACY_TO_API.items():
        if old_key in cfg:
            new_cfg["api"][new_key] = cfg.pop(old_key)
    for old_key, new_key in LEGACY_TO_SCHEMA.items():
        if old_key in cfg:
            new_cfg["schema"][new_key] = cfg.pop(old_key)
    for old_key, new_key in LEGACY_TO_PROCESSING.items():
        if old_key in cfg:
            new_cfg["processing"][new_key] = cfg.pop(old_key)

    # --- special cases -------------------------------------------------------
    if "related_fields" in cfg:
        new_cfg["schema"]["related_tables"] = cfg.pop("related_fields")

    # If related_tables already present keep it
    if "related_tables" in cfg:
        new_cfg["schema"]["related_tables"] = cfg.pop("related_tables")

    # description is kept at root if present
    if "description" in cfg:
        new_cfg["description"] = cfg.pop("description")

    # Anything left over that we did not recognise – keep at root to avoid loss
    for k, v in cfg.items():
        new_cfg.setdefault(k, v)

    # Ensure subsections exist even if empty (loader relies on keys present)
    for sect in ("api", "schema", "processing"):
        new_cfg.setdefault(sect, {})

    return new_cfg


def migrate_file(path: Path, dry_run: bool = False) -> None:
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    if data and isinstance(data, list):
        converted: List[Dict[str, Any]] = [
            convert_single_config(d.copy()) for d in data
        ]
    elif data and isinstance(data, dict):
        converted = [convert_single_config(data.copy())]
    else:
        print(f"[WARN] Skipping {path} – unrecognised YAML structure", file=sys.stderr)
        return

    if dry_run:
        print(f"--- {path.relative_to(PROJECT_ROOT)} (dry-run) ---")
        print(yaml.safe_dump(converted, sort_keys=False, indent=2))
        return

    with open(path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(converted, fh, sort_keys=False, indent=2)
    print(f"Converted {path.relative_to(PROJECT_ROOT)} → new format")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Convert legacy Congressional configs")
    parser.add_argument(
        "--dry-run", action="store_true", help="show output without overwriting files"
    )
    args = parser.parse_args(argv)

    yaml_files = list(CFG_GLOB.rglob("config.yaml"))
    if not yaml_files:
        print("No Congressional config.yaml files found", file=sys.stderr)
        sys.exit(1)

    for fp in yaml_files:
        migrate_file(fp, dry_run=args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
