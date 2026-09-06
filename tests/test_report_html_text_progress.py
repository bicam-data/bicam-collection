from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.report_html_text_progress import TARGET_SQL, render_markdown


def test_progress_query_includes_govinfo_html_renditions() -> None:
    assert "api\\.govinfo\\.gov/.+/htm" in TARGET_SQL
    assert "'bills_versions'" in TARGET_SQL
    assert 'excluded_non_html_count' in TARGET_SQL
    assert 'source_absent_count' in TARGET_SQL
    assert 'fetch_error_count' in TARGET_SQL
    assert 'empty_count' in TARGET_SQL
    assert "url !~* '/crec/'" in TARGET_SQL
    assert "url ~* '/crec/'" in TARGET_SQL
    assert 'declared_source_tables' in TARGET_SQL
    assert "('committee_prints_texts'::text)" in TARGET_SQL
    assert 'count(filtered.url)' in TARGET_SQL


def test_render_markdown_lists_per_table_progress() -> None:
    rendered = render_markdown(
        {
            'generated_at': '2026-09-01T00:00:00+00:00',
            'tables': [
                {
                    'source_table': 'bills_texts',
                    'target_count': 10,
                    'success_count': 7,
                    'non_success_count': 1,
                    'source_absent_count': 1,
                    'http_error_count': 0,
                    'fetch_error_count': 0,
                    'empty_count': 0,
                    'pending_count': 2,
                    'excluded_non_html_count': 0,
                    'attempt_count_6h': 12,
                    'attempt_rate_per_hour': 2.0,
                    'sweep_eta_hours': 1.0,
                    'success_count_6h': 6,
                    'success_rate_per_hour': 1.0,
                    'success_eta_hours': 2.0,
                }
            ],
        }
    )
    assert 'BICAM HTML text extraction progress' in rendered
    assert '| bills_texts | 10 | 7 | 1 | 1 | 0 | 0 | 0 | 2 | 0 | 12 | 2.0 | 1.0 | 6 | 2.0 |' in rendered


def test_progress_report_surfaces_excluded_non_html_links() -> None:
    assert 'excluded_non_html_count' in TARGET_SQL
    rendered = render_markdown(
        {
            'generated_at': '2026-09-01T00:00:00+00:00',
            'tables': [
                {
                    'source_table': 'hearings_texts',
                    'target_count': 10,
                    'success_count': 10,
                    'non_success_count': 0,
                    'source_absent_count': 0,
                    'http_error_count': 0,
                    'fetch_error_count': 0,
                    'empty_count': 0,
                    'pending_count': 0,
                    'excluded_non_html_count': 1,
                }
            ],
        }
    )
    assert '| hearings_texts | 10 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 1 |' in rendered
