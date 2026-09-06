from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_html_checkpoint_rebuilds_resolver_ancestors() -> None:
    script = (REPO_ROOT / "scripts" / "run_html_text_batches.sh").read_text(encoding="utf-8")

    assert "selected child-state ancestors" in script
    assert "html_dbt_select=(" in script
    assert '"${html_dbt_select[@]}"' in script
    assert "int_congress_child_response_selection" in script
    assert "int_congress_child_response_selections" in script
    assert "stg_congress__committee_report_detail" in script
    assert "committee_prints_texts" in script
    assert "committee_reports_texts" in script
    assert "hearings_texts" in script
    assert "bills_versions" in script
    assert "--select +bills_texts" not in script


def test_html_checkpoint_keeps_cursor_when_propagation_times_out() -> None:
    script = (REPO_ROOT / "scripts" / "run_html_text_batches.sh").read_text(encoding="utf-8")

    assert "html_dbt_run_status" in script
    assert "html_dbt_pending=1" in script
    assert "statement_timeout" in script
    assert "PGAPPNAME=\"$html_dbt_app_name\"" in script
    assert "cancel_html_dbt_backends" in script
    assert "trap cleanup_html_dbt_backends EXIT" in script
    assert "trap 'exit 143' TERM" in script
    assert "cancel_attempt in 1 2" in script
    assert "pg_terminate_backend(pid)" in script
    assert 'payload["pending_dbt"] = 0' in script


def test_dbt_profile_accepts_run_specific_application_names() -> None:
    profiles = (REPO_ROOT / "dbt_project" / "profiles.yml").read_text(encoding="utf-8")

    assert 'application_name: "{{ env_var(\'PGAPPNAME\', \'dbt\') }}"' in profiles


def test_html_checkpoint_amortizes_expensive_dbt_propagation() -> None:
    script = (REPO_ROOT / "scripts" / "run_html_text_batches.sh").read_text(encoding="utf-8")

    assert "BICAM_HTML_DBT_BATCH_INTERVAL" in script
    assert "dbt_batches_since_last" in script
    assert "html_dbt_deferred=1" in script


def test_html_propagation_has_a_bounded_production_window() -> None:
    script = (REPO_ROOT / "scripts" / "run_html_text_batches.sh").read_text(encoding="utf-8")

    assert "BICAM_HTML_DBT_TIMEOUT_SECONDS:-1800" in script
    assert "BICAM_HTML_DBT_STATEMENT_TIMEOUT_MS:-1740000" in script
