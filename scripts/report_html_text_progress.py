"""Write durable per-table progress for the bounded HTML extraction lane."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile

import psycopg

from bicam_collection.config import Settings


TARGET_SQL = r"""
with all_targets as (
    select 'bills_texts'::text as source_table, formatted_text as url
    from bicam.bills_texts
    where formatted_text is not null
    union all
    select 'committee_reports_texts', formatted_text
    from bicam.committee_reports_texts
    where formatted_text is not null
    union all
    select 'hearings_texts', formatted_text
    from bicam.hearings_texts
    where formatted_text is not null
    union all
    select 'bills_versions', formatted_text
    from bicam.bills_versions
    where formatted_text is not null
    union all
    select 'amendments_texts', html
    from bicam.amendments_texts
    where html is not null
    union all
    select 'committee_prints_texts', html
    from bicam.committee_prints_texts
    where html is not null
), filtered as (
    select distinct source_table, url
    from all_targets
    where url ~* '^https://((www\.)?congress\.gov/.+\.html?(\?|$)|api\.govinfo\.gov/.+/htm(\?|$))'
      and url !~* '/crec/'
), excluded as (
    select source_table, count(distinct url)::bigint as excluded_non_html_count
    from all_targets
    where not (url ~* '^https://((www\.)?congress\.gov/.+\.html?(\?|$)|api\.govinfo\.gov/.+/htm(\?|$))')
       or url ~* '/crec/'
    group by source_table
), declared_source_tables as (
    select source_table
    from (values
        ('amendments_texts'::text),
        ('bills_texts'::text),
        ('bills_versions'::text),
        ('committee_prints_texts'::text),
        ('committee_reports_texts'::text),
        ('hearings_texts'::text)
    ) as values_table(source_table)
), counts as (
    select
        source_tables.source_table,
        count(filtered.url)::bigint as target_count,
        count(filtered.url) filter (where current.extraction_status = 'success')::bigint as success_count,
        count(filtered.url) filter (where current.id is not null and current.extraction_status <> 'success')::bigint as non_success_count,
        count(filtered.url) filter (
            where current.extraction_status = 'http_error'
              and current.http_status in (404, 410)
        )::bigint as source_absent_count,
        count(filtered.url) filter (
            where current.extraction_status = 'http_error'
              and current.http_status not in (404, 410)
        )::bigint as http_error_count,
        count(filtered.url) filter (where current.extraction_status = 'fetch_error')::bigint as fetch_error_count,
        count(filtered.url) filter (where current.extraction_status = 'empty')::bigint as empty_count,
        count(filtered.url) filter (where current.id is null)::bigint as pending_count,
        max(current.fetched_at) as last_fetched_at
    from declared_source_tables as source_tables
    left join filtered using (source_table)
    left join public.html_text_current as current using (url)
    group by source_tables.source_table
), recent_attempts as (
    select
        filtered.source_table,
        count(*)::bigint as attempt_count_6h,
        count(*) filter (where versions.extraction_status = 'success')::bigint as success_count_6h
    from filtered
    join public.html_text_versions as versions using (url)
    where versions.fetched_at >= now() - interval '6 hours'
    group by filtered.source_table
)
select
    counts.source_table,
    counts.target_count,
    counts.success_count,
    counts.non_success_count,
    counts.source_absent_count,
    counts.http_error_count,
    counts.fetch_error_count,
    counts.empty_count,
    counts.pending_count,
    counts.last_fetched_at,
    coalesce(excluded.excluded_non_html_count, 0)::bigint as excluded_non_html_count,
    coalesce(recent_attempts.attempt_count_6h, 0)::bigint as attempt_count_6h,
    case
        when coalesce(recent_attempts.attempt_count_6h, 0) > 0
        then round((recent_attempts.attempt_count_6h / 6.0)::numeric, 1)::double precision
        else null
    end as attempt_rate_per_hour,
    case
        when coalesce(recent_attempts.attempt_count_6h, 0) > 0
        then round(
            counts.pending_count / (recent_attempts.attempt_count_6h / 6.0)::numeric,
            1
        )::double precision
        else null
    end as sweep_eta_hours,
    coalesce(recent_attempts.success_count_6h, 0)::bigint as success_count_6h,
    case
        when coalesce(recent_attempts.success_count_6h, 0) > 0
        then round((recent_attempts.success_count_6h / 6.0)::numeric, 1)::double precision
        else null
    end as success_rate_per_hour,
    case
        when coalesce(recent_attempts.success_count_6h, 0) > 0
        then round(
            counts.pending_count / (recent_attempts.success_count_6h / 6.0)::numeric,
            1
        )::double precision
        else null
    end as success_eta_hours
from counts
left join recent_attempts using (source_table)
left join excluded using (source_table)
order by counts.source_table
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=Path('../reports/html-text'))
    parser.add_argument('--database-url')
    return parser.parse_args(argv)


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=path.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)


def build_report(database_url: str) -> dict[str, object]:
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(TARGET_SQL).fetchall()
    columns = [
        'source_table',
        'target_count',
        'success_count',
        'non_success_count',
        'source_absent_count',
        'http_error_count',
        'fetch_error_count',
        'empty_count',
        'pending_count',
        'last_fetched_at',
        'excluded_non_html_count',
        'attempt_count_6h',
        'attempt_rate_per_hour',
        'sweep_eta_hours',
        'success_count_6h',
        'success_rate_per_hour',
        'success_eta_hours',
    ]
    return {
        # Increment when the progress artifact gains a field so downstream
        # checkpoint readers can distinguish the richer report shape.
        'schema_version': 5,
        'generated_at': datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        'tables': [dict(zip(columns, row, strict=True)) for row in rows],
    }


def render_markdown(report: dict[str, object]) -> str:
    lines = [
        '# BICAM HTML text extraction progress',
        '',
        f"Generated: `{report['generated_at']}`",
        '',
        '| Source table | HTML targets | Success | Non-success | Source absent | HTTP error | Fetch error | Empty | Pending | Excluded non-HTML/paused | Attempts (6h) | Rate/hr | Sweep ETA (h) | Success (6h) | Success ETA (h) |',
        '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
    ]
    for row in report['tables']:
        assert isinstance(row, dict)
        lines.append(
            f"| {row['source_table']} | {row['target_count']} | {row['success_count']} | "
            f"{row['non_success_count']} | {row.get('source_absent_count', 0)} | "
            f"{row.get('http_error_count', 0)} | {row.get('fetch_error_count', 0)} | "
            f"{row.get('empty_count', 0)} | {row['pending_count']} | {row.get('excluded_non_html_count', 0)} | "
            f"{row.get('attempt_count_6h', 0)} | {row.get('attempt_rate_per_hour') or '—'} | "
            f"{row.get('sweep_eta_hours') or '—'} | {row.get('success_count_6h', 0)} | "
            f"{row.get('success_eta_hours') or '—'} |"
        )
    lines.append('')
    return '\n'.join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    database_url = args.database_url or Settings.from_env(Path(__file__).resolve().parents[1]).database_url
    report = build_report(database_url)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(report, indent=2, default=_json_default) + '\n'
    _write_atomic(args.output_dir / 'progress.json', rendered)
    _write_atomic(args.output_dir / 'progress.md', render_markdown(report))
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    _write_atomic(args.output_dir / f'{stamp}.json', rendered)
    total = sum(int(row['target_count']) for row in report['tables'])
    success = sum(int(row['success_count']) for row in report['tables'])
    print(f'html_text_progress targets={total} success={success} output={args.output_dir}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
