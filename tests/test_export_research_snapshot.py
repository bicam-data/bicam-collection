from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "export_research_snapshot.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("export_research_snapshot", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_export_scope_is_explicit_and_covers_main11() -> None:
    script = _load_module()
    assert set(script.CORE_EXPORTS) == {
        "bills",
        "amendments",
        "members",
        "congresses",
        "hearings",
        "committees",
        "committee_meetings",
        "committee_prints",
        "committee_reports",
        "treaties",
        "nominations",
    }
    args = script.parse_args(["--tables", "bills", "committee_reports", "--dry-run"])
    assert args.tables == ["bills", "committee_reports"]
    assert args.dry_run is True


def test_export_scope_is_contract_only() -> None:
    script = _load_module()

    assert script.RELATION_EXPORTS == {}
    assert len(script.EXPORTS) == 89
    assert "bills_lobbied" not in script.EXPORTS


def test_supplemental_export_is_explicit() -> None:
    script = _load_module()
    _, relations = script._contract_plan(
        script.DEFAULT_CONTRACT,
        None,
        include_supplemental=True,
    )
    assert [relation["table"] for relation in relations[-2:]] == [
        "bills_versions",
        "bills_version_links",
    ]
    assert script.parse_args(["--include-supplemental"]).include_supplemental is True
    args = script.parse_args(["--include-source-evidence", "--source-evidence-limit", "7"])
    assert args.include_source_evidence is True
    assert args.source_evidence_limit == 7


def test_obsolete_bridge_names_are_not_export_relations() -> None:
    script = _load_module()
    for name in ("member_legislation", "committee_bills"):
        try:
            script._contract_plan(script.DEFAULT_CONTRACT, [name])
        except ValueError as error:
            assert "unknown core contract relation" in str(error)
        else:
            raise AssertionError(f"obsolete bridge name unexpectedly exportable: {name}")


def test_quote_identifier_rejects_qualified_or_empty_names() -> None:
    script = _load_module()
    assert script.quote_identifier("int_bills__congress") == '"int_bills__congress"'
    for value in ("", "public.int_bills__congress", "bad\x00name"):
        try:
            script.quote_identifier(value)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for {value!r}")


def test_dry_run_is_database_free_and_preserves_order(tmp_path: Path) -> None:
    script = _load_module()
    generated_at = datetime(2026, 8, 28, 6, 10, tzinfo=timezone.utc)
    result = script.export_snapshot(
        database_url="postgresql://unused",
        output_dir=tmp_path,
        table_names=["bills", "members"],
        generated_at=generated_at,
        dry_run=True,
    )
    assert result["generated_at"] == "2026-08-28T06:10:00+00:00"
    assert result["contract_version"] == 3
    assert result["schema"] == "bicam"
    assert result["scope"]["tables"] == [
        {"name": "bills", "relation": "bills"},
        {"name": "members", "relation": "members"},
    ]
    assert result["serialization"] == {
        "format": "csv.gz",
        "encoding": "UTF-8",
        "newline": "LF",
        "header": True,
        "csv_null": r"\N",
    }
    assert result["dry_run"] is True
    assert not list(tmp_path.iterdir())


def test_lineage_columns_are_explicit() -> None:
    script = _load_module()
    assert "source_raw_response_version_id" in script.LINEAGE_COLUMNS
    assert "raw_response_version_id" in script.LINEAGE_COLUMNS
    assert "bioguide_id" not in script.LINEAGE_COLUMNS


def test_lineage_columns_include_relationship_arrays_and_evidence() -> None:
    script = _load_module()
    assert script.lineage_columns(
        [
            "canonical_bill_id",
            "source_raw_ids",
            "source_raw_response_version_ids",
            "source_endpoint_urls",
            "evidence_link_ids",
            "bioguide_id_raw",
        ]
    ) == [
        "source_raw_ids",
        "source_raw_response_version_ids",
        "source_endpoint_urls",
        "evidence_link_ids",
    ]


def test_source_bundle_evidence_has_allowlist_and_dependency_identity() -> None:
    script = _load_module()
    bundle = script._source_bundle_evidence(PROJECT_ROOT)

    assert bundle["schema_version"] == 1
    assert bundle["reproduction_level"] == "metadata-bound; raw payload bundle not included"
    assert "src" in bundle["source_allowlist"]
    assert "uv.lock" in bundle["source_allowlist"]
    assert bundle["source_tree_sha256"]
    assert bundle["dependency_lock"]["path"] == "uv.lock"
    assert bundle["dependency_lock"]["sha256"]
    assert bundle["local_dependencies"]
    assert bundle["local_dependencies"][0]["path"] == "rotisserie"


class _BuildEvidenceConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, query, params=None):  # noqa: ANN001 - tiny SQL response stub
        text = str(query)
        self.queries.append(text)
        if "from pg_class" in text:
            return self
        if "fct_reconciliation_metrics" in text:
            self.row = (3, 3, 0)
        elif "fct_congress_internal_reciprocity_metrics" in text:
            self.row = (11, 11, 0)
        elif "fct_root_detail_coverage" in text:
            self.row = (0,)
        elif "fct_collection_coverage" in text:
            self.row = (0,)
        elif "fct_source_gap_queue" in text:
            self.row = (0, 12)
        elif "raw_response_versions" in text:
            self.rows = [("congress", "bills", datetime(2026, 9, 5, tzinfo=timezone.utc), 4)]
        return self

    def fetchone(self):
        if hasattr(self, "row"):
            return self.row
        return (True,)

    def fetchall(self):
        return getattr(self, "rows", [])


def test_build_evidence_binds_quality_and_frontier_to_export_transaction(tmp_path: Path, monkeypatch) -> None:
    script = _load_module()
    contract_path = PROJECT_ROOT / "contracts" / "bicam_public_schema.yaml"
    contract = script.load_contract(contract_path)
    monkeypatch.setattr(script, "validate_product_data", lambda *args, **kwargs: {"valid": True, "checks": []})
    connection = _BuildEvidenceConnection()

    evidence = script._capture_build_evidence(
        connection,
        schema="bicam",
        contract=contract,
        contract_path=contract_path,
        project_root=PROJECT_ROOT,
        table_records=[{"name": "bills", "row_count": 1}],
        database_snapshot_at=datetime(2026, 9, 5, tzinfo=timezone.utc),
    )

    assert evidence["status"] == "complete"
    assert evidence["quality_status"] == "pass"
    assert evidence["build_id"]
    assert evidence["source_bundle_sha256"]
    assert evidence["source_bundle"]["schema_version"] == 1
    assert evidence["source_frontiers"][0]["category"] == "bills"
    assert any('"public"."fct_source_gap_queue"' in query for query in connection.queries)
    assert any(
        '"public"."fct_root_detail_coverage"' in query and "category = any(%s)" in query
        for query in connection.queries
    )


def test_ci_export_smoke_uses_contract_relation_names() -> None:
    workflow = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "--dry-run --tables bills committee_reports" in workflow
    assert "--dry-run --tables member_legislation committee_bills" not in workflow


def test_ci_materializes_seeded_complete_public_graph_before_quality_smoke() -> None:
    workflow = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    seed_step = "uv run dbt seed --project-dir dbt_project --full-refresh --no-partial-parse"
    graph_step = (
        "uv run dbt run --project-dir dbt_project --select '+path:models/marts/bicam' "
        "'+fct_reconciliation_metrics' '+fct_congress_internal_reciprocity_metrics' "
        "'+fct_root_detail_coverage' '+fct_collection_coverage' '+fct_source_gap_queue' "
        "'+bridge_committees__bills' --no-partial-parse"
    )
    validator_step = "uv run python scripts/validate_bicam_product.py --schema bicam --include-supplemental --check-data"
    assert seed_step in workflow
    assert graph_step in workflow
    assert validator_step in workflow
    assert workflow.index(seed_step) < workflow.index(graph_step)


def test_ci_exercises_exact_candidate_export_validation_and_safe_drift_rejection() -> None:
    workflow = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "Export complete disposable candidate" in workflow
    assert "--include-supplemental" in workflow
    assert "scripts/promote_research_snapshot.py" in workflow
    assert "Reject drifted disposable candidate" in workflow
    assert 'manifest["snapshot_directory"] = "invalid-candidate"' in workflow
    assert 'manifest["tables"][0]["sha256"] = "0" * 64' in workflow
    assert 'test "$(readlink "$BICAM_CI_EXPORT_DIR/latest")" = "$accepted"' in workflow
    assert "Compare exact disposable candidate" in workflow
    assert "scripts/compare_release_candidates.py" in workflow
