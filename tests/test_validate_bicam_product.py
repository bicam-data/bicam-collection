from __future__ import annotations

import importlib.util
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "validate_bicam_product.py"
SPEC = importlib.util.spec_from_file_location("validate_bicam_product", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_contract_core_relations_exclude_deferred_relation() -> None:
    contract = MODULE.load_contract(PROJECT_ROOT / "contracts" / "bicam_public_schema.yaml")
    relations = MODULE.contract_relations(contract)
    names = [relation["table"] for relation in relations]

    assert len(names) == 89
    assert "bills_lobbied" not in names
    assert names[:3] == ["bills", "bills_actions", "bills_actions_committees"]


def test_compare_reports_missing_extra_order_and_numeric_type() -> None:
    contract = {
        "contract_version": 1,
        "contract_kind": "public_product_schema",
        "families": [
            {
                "table": "bills",
                "feature_status": "core-now",
                "columns": {"bill_id": "text", "bill_number": "numeric"},
                "primary_key": ["bill_id"],
            }
        ],
    }
    actual = {
        "bills": [
            {"name": "bill_number", "data_type": "numeric", "udt_name": "numeric", "is_nullable": "YES"},
            {"name": "bill_id", "data_type": "text", "udt_name": "text", "is_nullable": "YES"},
        ],
        "bills_lobbied": [],
    }

    report = MODULE.compare_schema(contract, actual)

    assert report["valid"] is False
    assert report["missing_relations"] == []
    assert report["extra_relations"] == ["bills_lobbied"]
    relation = report["relations"][0]
    assert relation["column_order_matches"] is False
    assert relation["type_mismatches"] == []


def test_compare_honors_explicit_nullability_without_inventing_constraints() -> None:
    contract = {
        "contract_version": 1,
        "contract_kind": "public_product_schema",
        "families": [
            {
                "table": "bills",
                "feature_status": "core-now",
                "not_null_columns": ["bill_id"],
                "columns": {"bill_id": "text"},
                "primary_key": ["bill_id"],
            }
        ],
    }
    report = MODULE.compare_schema(
        contract,
        {"bills": [{"name": "bill_id", "data_type": "text", "is_nullable": "YES"}]},
    )

    assert report["relations"][0]["nullability_mismatches"] == [
        {"column": "bill_id", "expected_nullable": False, "actual_nullable": True}
    ]


def test_contract_file_is_yaml_and_targets_bicam() -> None:
    document = yaml.safe_load((PROJECT_ROOT / "contracts" / "bicam_public_schema.yaml").read_text())
    assert document["product"]["postgres_schema"] == "bicam"


def test_declared_supplemental_relation_is_allowed_without_changing_core_count() -> None:
    contract = MODULE.load_contract(PROJECT_ROOT / "contracts" / "bicam_public_schema.yaml")
    assert "bills_versions" not in {row["table"] for row in MODULE.contract_relations(contract)}
    supplemental = MODULE.contract_relations(contract, include_supplemental=True)
    assert [relation["table"] for relation in supplemental[-2:]] == [
        "bills_versions",
        "bills_version_links",
    ]
    report = MODULE.compare_schema(contract, {"bills_versions": []})
    assert report["extra_relations"] == []
    assert report["supplemental_relations"] == ["bills_versions"]


def test_cli_can_require_supplemental_relations() -> None:
    script = MODULE
    assert script.parse_args(["--include-supplemental"]).include_supplemental is True
    args = script.parse_args(["--check-data", "--max-examples", "3"])
    assert args.check_data is True
    assert args.max_examples == 3


def test_include_supplemental_changes_expected_relation_set() -> None:
    contract = MODULE.load_contract(PROJECT_ROOT / "contracts" / "bicam_public_schema.yaml")
    report = MODULE.compare_schema(
        contract,
        {relation["table"]: [] for relation in MODULE.contract_relations(contract, include_supplemental=True)},
        include_supplemental=True,
    )
    assert report["expected_relation_count"] == 91
    assert report["expected_relations"][-2:] == ["bills_versions", "bills_version_links"]


class _EmptyConnection:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, query, params):  # noqa: ANN001 - deliberately tiny DB stub
        self.calls += 1
        return self

    def fetchall(self):
        return []


def test_validate_product_data_executes_declared_key_and_required_checks() -> None:
    contract = {
        "contract_version": 1,
        "contract_kind": "public_product_schema",
        "families": [
            {
                "table": "bills",
                "feature_status": "core-now",
                "columns": {"bill_id": "text"},
                "primary_key": ["bill_id"],
                "required_columns": ["bill_id"],
            }
        ],
    }
    connection = _EmptyConnection()

    report = MODULE.validate_product_data(connection, contract)

    assert report["valid"] is True
    assert report["failure_count"] == 0
    assert {check["check_id"] for check in report["checks"]} == {
        "required_value:bills.bill_id",
        "key_nonnull:bills.[bill_id]",
        "key_unique:bills.[bill_id]",
    }
    assert connection.calls == 3


def test_validate_product_data_supports_declared_composite_foreign_keys() -> None:
    contract = {
        "contract_version": 1,
        "contract_kind": "public_product_schema",
        "families": [
            {
                "table": "parents",
                "feature_status": "core-now",
                "columns": {"parent_id": "text", "part": "integer"},
                "primary_key": ["parent_id", "part"],
            },
            {
                "table": "children",
                "feature_status": "core-now",
                "columns": {"parent_id": "text", "part": "integer"},
                "primary_key": ["parent_id", "part"],
                "foreign_key_constraints": [
                    {
                        "local_columns": ["parent_id", "part"],
                        "target_relation": "parents",
                        "target_columns": ["parent_id", "part"],
                    }
                ],
            },
        ],
    }

    report = MODULE.validate_product_data(_EmptyConnection(), contract)

    assert report["valid"] is True
    assert "foreign_key:children.[parent_id,part]->parents.[parent_id,part]" in {
        check["check_id"] for check in report["checks"]
    }


def test_composite_foreign_key_shape_must_be_explicit_and_aligned() -> None:
    relation = {
        "foreign_key_constraints": [
            {
                "local_columns": ["parent_id", "part"],
                "target_relation": "parents",
                "target_columns": ["parent_id"],
            }
        ]
    }

    try:
        MODULE._composite_foreign_keys(relation)
    except ValueError as error:
        assert "column counts" in str(error)
    else:  # pragma: no cover - assertion documents the fail-closed contract
        raise AssertionError("misaligned composite foreign keys must be rejected")


def test_validate_product_data_fails_closed_for_malformed_composite_declaration() -> None:
    contract = {
        "contract_version": 1,
        "contract_kind": "public_product_schema",
        "families": [
            {
                "table": "children",
                "feature_status": "core-now",
                "columns": {"parent_id": "text"},
                "foreign_key_constraints": {"local_columns": ["parent_id"]},
            }
        ],
    }

    report = MODULE.validate_product_data(_EmptyConnection(), contract)

    assert report["valid"] is False
    assert report["failures"][0]["check_id"] == "contract.composite_foreign_key_declared:children"


def test_validate_product_data_fails_closed_for_missing_scalar_fk_columns() -> None:
    contract = {
        "contract_version": 1,
        "contract_kind": "public_product_schema",
        "families": [
            {
                "table": "parents",
                "feature_status": "core-now",
                "columns": {"parent_id": "text"},
                "primary_key": ["parent_id"],
            },
            {
                "table": "children",
                "feature_status": "core-now",
                "columns": {"parent_id": "text"},
                "foreign_keys": {
                    "missing_local": "parents.parent_id",
                    "parent_id": "parents.missing_target",
                },
            },
        ],
    }

    report = MODULE.validate_product_data(_EmptyConnection(), contract)

    assert report["valid"] is False
    assert {failure["check_id"] for failure in report["failures"]} == {
        "contract.foreign_key_local_column:children.missing_local",
        "contract.foreign_key_target_column:children.parent_id",
    }
