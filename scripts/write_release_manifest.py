"""Write a machine-readable release manifest for the production main-11 archive.

The manifest is deliberately read-only.  It snapshots materialized dbt
coverage, reconciliation, reciprocity, and GovInfo identity facts in one
durable artifact so a later process (or a later usage window) can verify what
was known at release time without rerunning a multi-million-row query.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import tempfile
from typing import Any

import psycopg

from bicam_collection.config import Settings
from bicam_collection.target_scope import TARGET_CONGRESS_CATEGORIES


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("../reports/main-11-release-manifest-latest.json"),
        help="Path for the latest manifest (default: ../reports/main-11-release-manifest-latest.json).",
    )
    parser.add_argument(
        "--history-dir",
        type=Path,
        default=Path("../reports/release-manifests"),
        help="Directory for timestamped immutable manifest copies.",
    )
    return parser.parse_args(argv)


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _records(rows: list[tuple], columns: tuple[str, ...]) -> list[dict[str, Any]]:
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _fetch_manifest(connection: psycopg.Connection) -> dict[str, Any]:
    categories = list(TARGET_CONGRESS_CATEGORIES)
    scope_congress_end = connection.execute(
        """
        select coalesce(
            max((regexp_match(endpoint_url, '/congress/([0-9]+)(?:[?/]|$)'))[1]::integer),
            119
        )
          from raw_responses
         where source = 'congress'
           and category = 'congresses'
           and node_type = 'congress_detail'
        """
    ).fetchone()[0]
    root_rows = connection.execute(
        """
        select category,
               sum(root_item_count)::bigint as root_item_count,
               sum(fetched_detail_count)::bigint as fetched_detail_count,
               sum(missing_detail_count)::bigint as missing_detail_count,
               sum(confirmed_source_error_detail_count)::bigint
                   as confirmed_source_error_detail_count,
               sum(actionable_missing_detail_count)::bigint
                   as actionable_missing_detail_count,
               max(last_fetched_at) as last_fetched_at,
               bool_and(quality_status = 'pass') as quality_pass
        from fct_root_detail_coverage
        where source = 'congress' and category = any(%s)
        group by category
        order by category
        """,
        (categories,),
    ).fetchall()
    child_rows = connection.execute(
        """
        select category,
               sum(expected_child_fetch_count)::bigint as expected_child_fetch_count,
               sum(child_successful_fetch_count)::bigint as child_successful_fetch_count,
               sum(child_failed_fetch_count)::bigint as child_failed_fetch_count,
               sum(child_unsatisfied_fetch_count)::bigint
                   as child_unsatisfied_fetch_count,
               max(last_fetched_at) as last_fetched_at
        from fct_collection_coverage
        where source = 'congress' and category = any(%s)
        group by category
        order by category
        """,
        (categories,),
    ).fetchall()
    pagination_rows = connection.execute(
        """
        select category,
               sum(expected_item_count)::bigint as expected_item_count,
               sum(enumerated_item_count)::bigint as enumerated_item_count,
               sum(missing_item_count)::bigint as missing_item_count,
               sum(confirmed_source_error_item_count)::bigint
                   as confirmed_source_error_item_count,
               sum(actionable_missing_item_count)::bigint
                   as actionable_missing_item_count,
               max(list_page_count)::bigint as list_page_count
        from fct_pagination_coverage
        where category = any(%s)
        group by category
        order by category
        """,
        (categories,),
    ).fetchall()
    reconciliation_rows = connection.execute(
        """
        select entity, authoritative_source, contract_version,
               candidate_count, accepted_count, rejected_count,
               ambiguous_count, unmatched_count,
               authoritative_record_count, authoritative_accepted_count,
               authoritative_source_gap_count,
               eligible_authoritative_record_count,
               eligible_authoritative_accepted_count,
               open_exception_count, reviewed_exception_count,
               unexcused_unmatched_count, unexcused_rejected_count,
               unexcused_ambiguous_count,
               ambiguous_rate, authoritative_match_rate,
               eligible_authoritative_match_rate, title_agreement_rate,
               quality_status
        from fct_reconciliation_metrics
        where entity in ('bill', 'committee_report', 'hearing')
        order by entity
        """,
    ).fetchall()
    reciprocity_rows = connection.execute(
        """
        select relationship_type, relationship_count, reciprocal_count,
               unavailable_count, asymmetric_count, contradiction_count,
               unclassified_count, reciprocal_rate, contradiction_rate,
               quality_status,
               case
                   when relationship_count = 0 then 'reject_no_evidence'
                   when contradiction_count > 0 then 'reject_contradiction'
                   when unclassified_count > 0 then 'reject_unclassified'
                   else 'accepted_with_source_boundaries'
               end as release_acceptance_status
        from fct_congress_internal_reciprocity_metrics
        order by relationship_type
        """,
    ).fetchall()
    release_bridge_rows = connection.execute(
        """
        select bridge_name,
               row_count,
               two_sided_count,
               one_sided_count,
               asymmetric_count,
               unavailable_count
        from (
            select
                'member_legislation'::text as bridge_name,
                count(*)::bigint as row_count,
                count(*) filter (
                    where member_side_present and legislation_side_present
                )::bigint as two_sided_count,
                count(*) filter (
                    where not member_side_present or not legislation_side_present
                )::bigint as one_sided_count,
                count(*) filter (
                    where position('asymmetric_source' in reciprocity_status) > 0
                )::bigint as asymmetric_count,
                count(*) filter (
                    where position('counterpart_unavailable' in reciprocity_status) > 0
                )::bigint as unavailable_count
            from bridge_members__legislation
            union all
            select
                'committee_bills'::text,
                count(*)::bigint,
                count(*) filter (
                    where committee_side_present and bill_side_present
                )::bigint,
                count(*) filter (
                    where not committee_side_present or not bill_side_present
                )::bigint,
                count(*) filter (
                    where position('asymmetric_source' in reciprocity_status) > 0
                )::bigint,
                count(*) filter (
                    where position('counterpart_unavailable' in reciprocity_status) > 0
                )::bigint
            from bridge_committees__bills
        ) as bridges
        order by bridge_name
        """,
    ).fetchall()
    govinfo_metrics = connection.execute(
        """
        select metric_name, metric_value, quality_status
        from fct_govinfo_data_quality_metrics
        order by metric_name
        """,
    ).fetchall()
    pending_identity = connection.execute(
        """
        select
            (select count(*) from fct_govinfo_member_identity_exceptions) as member_pending,
            (select count(*) from fct_govinfo_member_reference_identity_exceptions) as reference_pending,
            (select count(*) from fct_govinfo_nested_identity_exceptions) as nested_pending
        """,
    ).fetchone()
    print_metric = connection.execute(
        """
        select entity, contract_version, congress_print_count,
               govinfo_package_count, govinfo_representative_count,
               exact_match_count, congress_only_count,
               govinfo_only_representative_count, unparseable_package_count,
               duplicate_variant_package_count, excluded_non_cprt_count,
               exact_title_agreement_count, exact_match_rate, quality_status,
               observed_at
        from fct_committee_print_reconciliation_metrics
        where entity = 'committee_print'
        """,
    ).fetchall()
    window_rows = connection.execute(
        """
        select category, max(window_end) as completed_through,
               max(finished_at) as last_finished_at
        from govinfo_collection_windows
        where status = 'completed'
        group by category
        order by category
        """,
    ).fetchall()
    window_coverage_rows = connection.execute(
        """
        with ordered as (
            select
                category,
                window_start,
                window_end,
                status,
                lead(window_start) over (
                    partition by category order by window_start
                ) as next_window_start
            from govinfo_collection_windows
        )
        select
            category,
            count(*)::bigint as window_count,
            count(*) filter (where status = 'completed')::bigint
                as completed_window_count,
            count(*) filter (where status = 'failed')::bigint
                as failed_window_count,
            count(*) filter (where status = 'running')::bigint
                as running_window_count,
            min(window_start) as first_window_start,
            max(window_end) as last_window_end,
            count(*) filter (
                where next_window_start is not null
                  and next_window_start <> window_end
            )::bigint as gap_or_overlap_count,
            bool_and(status = 'completed') as all_windows_completed
        from ordered
        group by category
        order by category
        """,
    ).fetchall()
    govinfo_catalog_rows: list[tuple] = []
    govinfo_catalog_specs = (
        (
            "govinfo-bills",
            "govinfo_bill_package_list",
            "govinfo_bill_package_detail",
            None,
            None,
        ),
        (
            "govinfo-committee-prints",
            "govinfo_committee_print_package_list",
            "govinfo_committee_print_package_detail",
            "govinfo_committee_print_granule_list",
            "govinfo_committee_print_granule_detail",
        ),
        (
            "govinfo-committee-reports",
            "govinfo_committee_report_package_list",
            "govinfo_committee_report_package_detail",
            "govinfo_committee_report_granule_list",
            "govinfo_committee_report_granule_detail",
        ),
        (
            "govinfo-hearings",
            "govinfo_hearing_package_list",
            "govinfo_hearing_package_detail",
            "govinfo_hearing_granule_list",
            "govinfo_hearing_granule_detail",
        ),
    )
    for category, package_list_node, package_detail_node, granule_list_node, granule_detail_node in govinfo_catalog_specs:
        package_row = connection.execute(
            """
            with listed as materialized (
                select distinct upper(item->>'packageId') as package_id
                from raw_responses as responses
                cross join lateral jsonb_array_elements(
                    coalesce(responses.response->'packages', '[]'::jsonb)
                ) as item
                where responses.category = %s
                  and responses.node_type = %s
                  and item->>'packageId' is not null
            ), detailed as materialized (
                select distinct upper(response->>'packageId') as package_id
                from raw_responses
                where category = %s
                  and node_type = %s
                  and response->>'packageId' is not null
            )
            select
                (select count(*)::bigint from listed),
                (select count(*)::bigint from detailed),
                (select count(*)::bigint
                 from listed left join detailed using (package_id)
                 where detailed.package_id is null),
                (select count(*)::bigint
                 from detailed left join listed using (package_id)
                 where listed.package_id is null)
            """,
            (category, package_list_node, category, package_detail_node),
        ).fetchone()
        if granule_list_node is None or granule_detail_node is None:
            granule_row = (None, None, None, None)
        else:
            granule_row = connection.execute(
                """
                with listed as materialized (
                    select distinct upper(
                        coalesce(item->>'granuleId', item->>'granuleid')
                    ) as granule_id
                    from raw_responses as responses
                    cross join lateral jsonb_array_elements(
                        coalesce(responses.response->'granules', '[]'::jsonb)
                    ) as item
                    where responses.category = %s
                      and responses.node_type = %s
                      and coalesce(item->>'granuleId', item->>'granuleid') is not null
                ), detailed as materialized (
                    select distinct upper(response->>'granuleId') as granule_id
                    from raw_responses
                    where category = %s
                      and node_type = %s
                      and response->>'granuleId' is not null
                )
                select
                    (select count(*)::bigint from listed),
                    (select count(*)::bigint from detailed),
                    (select count(*)::bigint
                     from listed left join detailed using (granule_id)
                     where detailed.granule_id is null),
                    (select count(*)::bigint
                     from detailed left join listed using (granule_id)
                     where listed.granule_id is null)
                """,
                (category, granule_list_node, category, granule_detail_node),
            ).fetchone()
        govinfo_catalog_rows.append((category, *(package_row or (None, None, None, None)), *(granule_row or (None, None, None, None))))
    govinfo_collection_rows = connection.execute(
        """
        select
            category,
            upper(coalesce(response->>'collectionCode', '(missing)')) as collection_code,
            count(distinct upper(response->>'packageId'))::bigint as package_detail_count,
            min(nullif(response->>'congress', '')::integer) as oldest_congress,
            max(nullif(response->>'congress', '')::integer) as newest_congress,
            min(nullif(response->>'dateIssued', '')::date) as earliest_issued_date,
            max(nullif(response->>'dateIssued', '')::date) as latest_issued_date
        from raw_responses
        where source = 'govinfo'
          and node_type in (
              'govinfo_bill_package_detail',
              'govinfo_committee_print_package_detail',
              'govinfo_committee_report_package_detail',
              'govinfo_hearing_package_detail'
          )
          and response->>'packageId' is not null
        group by category, upper(coalesce(response->>'collectionCode', '(missing)'))
        order by category, collection_code
        """
    ).fetchall()
    source_error_rows = connection.execute(
        """
        select category, node_type, http_status, count(*)::bigint as failure_events,
               count(distinct endpoint_url)::bigint as distinct_urls
        from failed_requests
        where category = any(%s) and http_status in (404, 500)
        group by category, node_type, http_status
        order by category, node_type, http_status
        """,
        (categories,),
    ).fetchall()
    source_error_url_rows = connection.execute(
        """
        with latest as (
            select distinct on (failed.category, failed.endpoint_url, failed.node_type)
                failed.category,
                failed.node_type,
                failed.http_status,
                failed.endpoint_url,
                failed.failed_at,
                failed.error
            from failed_requests as failed
            where failed.category = any(%s)
              and failed.http_status = 500
              and failed.error like '%%source_error=%%'
            order by failed.category, failed.endpoint_url, failed.node_type,
                     failed.failed_at desc
        )
        select
            latest.category,
            latest.node_type,
            latest.http_status,
            latest.endpoint_url,
            latest.failed_at,
            substring(latest.error from 'source_error=([A-Za-z0-9_]+)') as source_error
        from latest
        left join raw_responses as responses
          on responses.endpoint_url = latest.endpoint_url
        where responses.id is null
        order by latest.category, latest.node_type, latest.endpoint_url
        """,
        (categories,),
    ).fetchall()
    html_text_rows = connection.execute(
        """
        select extraction_status, count(*)::bigint as row_count,
               count(*) filter (where nullif(trim(text_content), '') is not null)::bigint
                   as nonempty_text_count,
               max(fetched_at) as last_fetched_at
        from html_text_versions
        group by extraction_status
        order by extraction_status
        """
    ).fetchall()
    supplemental_bill_version_count = connection.execute(
        "select count(*)::bigint from bicam.bills_versions"
    ).fetchone()[0]
    bill_version_link_rows = connection.execute(
        """
        select source_table, link_status, count(*)::bigint
        from bicam.bills_version_links
        group by source_table, link_status
        order by source_table, link_status
        """
    ).fetchall()
    source_gap_queue_rows = connection.execute(
        """
        select queue_status, priority, count(*)::bigint
        from fct_source_gap_queue
        group by queue_status, priority
        order by queue_status, priority
        """
    ).fetchall()

    return {
        "schema_version": 1,
        "scope": {
            "source": "congress.gov",
            "categories": categories,
            "congress_start": 1,
            "congress_end": scope_congress_end,
            "semantics": "all source rows retained; source-side failures are classified, never synthesized",
        },
        "coverage": {
            "root_detail_by_category": _records(
                root_rows,
                (
                    "category",
                    "root_item_count",
                    "fetched_detail_count",
                    "missing_detail_count",
                    "confirmed_source_error_detail_count",
                    "actionable_missing_detail_count",
                    "last_fetched_at",
                    "quality_pass",
                ),
            ),
            "child_by_category": _records(
                child_rows,
                (
                    "category",
                    "expected_child_fetch_count",
                    "child_successful_fetch_count",
                    "child_failed_fetch_count",
                    "child_unsatisfied_fetch_count",
                    "last_fetched_at",
                ),
            ),
            "pagination_by_category": _records(
                pagination_rows,
                (
                    "category",
                    "expected_item_count",
                    "enumerated_item_count",
                    "missing_item_count",
                    "confirmed_source_error_item_count",
                    "actionable_missing_item_count",
                    "list_page_count",
                ),
            ),
        },
        "reconciliation": {
            "cross_source": _records(
                reconciliation_rows,
                (
                    "entity",
                    "authoritative_source",
                    "contract_version",
                    "candidate_count",
                    "accepted_count",
                    "rejected_count",
                    "ambiguous_count",
                    "unmatched_count",
                    "authoritative_record_count",
                    "authoritative_accepted_count",
                    "authoritative_source_gap_count",
                    "eligible_authoritative_record_count",
                    "eligible_authoritative_accepted_count",
                    "open_exception_count",
                    "reviewed_exception_count",
                    "unexcused_unmatched_count",
                    "unexcused_rejected_count",
                    "unexcused_ambiguous_count",
                    "ambiguous_rate",
                    "authoritative_match_rate",
                    "eligible_authoritative_match_rate",
                    "title_agreement_rate",
                    "quality_status",
                ),
            ),
            "internal_reciprocity": _records(
                reciprocity_rows,
                (
                    "relationship_type",
                    "relationship_count",
                    "reciprocal_count",
                    "unavailable_count",
                    "asymmetric_count",
                    "contradiction_count",
                    "unclassified_count",
                    "reciprocal_rate",
                    "contradiction_rate",
                    "quality_status",
                    "release_acceptance_status",
                ),
            ),
            "release_bridges": _records(
                release_bridge_rows,
                (
                    "bridge_name",
                    "row_count",
                    "two_sided_count",
                    "one_sided_count",
                    "asymmetric_count",
                    "unavailable_count",
                ),
            ),
        },
        "govinfo": {
            "identity_pending": dict(
                zip(
                    ("member", "reference", "nested"),
                    pending_identity or (None, None, None),
                    strict=True,
                )
            ),
            "data_quality_metrics": _records(
                govinfo_metrics, ("metric_name", "metric_value", "quality_status")
            ),
            "committee_print_reconciliation": _records(
                print_metric,
                (
                    "entity",
                    "contract_version",
                    "congress_print_count",
                    "govinfo_package_count",
                    "govinfo_representative_count",
                    "exact_match_count",
                    "congress_only_count",
                    "govinfo_only_representative_count",
                    "unparseable_package_count",
                    "duplicate_variant_package_count",
                    "excluded_non_cprt_count",
                    "exact_title_agreement_count",
                    "exact_match_rate",
                    "quality_status",
                    "observed_at",
                ),
            ),
            "completed_windows": _records(
                window_rows, ("category", "completed_through", "last_finished_at")
            ),
            "window_coverage": _records(
                window_coverage_rows,
                (
                    "category",
                    "window_count",
                    "completed_window_count",
                    "failed_window_count",
                    "running_window_count",
                    "first_window_start",
                    "last_window_end",
                    "gap_or_overlap_count",
                    "all_windows_completed",
                ),
            ),
            "catalog_coverage": _records(
                govinfo_catalog_rows,
                (
                    "category",
                    "package_listed_count",
                    "package_detail_count",
                    "package_listed_without_detail_count",
                    "package_detail_without_list_count",
                    "granule_listed_count",
                    "granule_detail_count",
                    "granule_listed_without_detail_count",
                    "granule_detail_without_list_count",
                ),
            ),
            "package_collection_breakdown": _records(
                govinfo_collection_rows,
                (
                    "category",
                    "collection_code",
                    "package_detail_count",
                    "oldest_congress",
                    "newest_congress",
                    "earliest_issued_date",
                    "latest_issued_date",
                ),
            ),
        },
        "known_source_failures": _records(
            source_error_rows,
            ("category", "node_type", "http_status", "failure_events", "distinct_urls"),
        ),
        "known_source_failure_urls": _records(
            source_error_url_rows,
            ("category", "node_type", "http_status", "endpoint_url", "failed_at", "source_error"),
        ),
        "supplemental": {
            "bill_version_count": supplemental_bill_version_count,
            "bill_version_links": _records(
                bill_version_link_rows,
                ("source_table", "link_status", "row_count"),
            ),
            "html_text_extractions": _records(
                html_text_rows,
                ("extraction_status", "row_count", "nonempty_text_count", "last_fetched_at"),
            ),
        },
        "source_gap_queue": _records(
            source_gap_queue_rows,
            ("queue_status", "priority", "row_count"),
        ),
    }


def build_manifest(
    *, generated_at: datetime, snapshot: dict[str, Any]
) -> dict[str, Any]:
    """Add a UTC generation timestamp without mutating the DB snapshot."""

    return {"generated_at": generated_at, **snapshot}


def write_manifest(
    *, database_url: str, output: Path, history_dir: Path, generated_at: datetime | None = None
) -> dict[str, Any]:
    generated_at = generated_at or datetime.now(timezone.utc)
    with psycopg.connect(database_url) as connection:
        connection.execute("set transaction read only")
        connection.execute("set statement_timeout='120s'")
        snapshot = _fetch_manifest(connection)
    manifest = build_manifest(generated_at=generated_at, snapshot=snapshot)
    rendered = json.dumps(manifest, default=_json_default, indent=2, sort_keys=True) + "\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=output.parent, delete=False) as temporary:
        temporary.write(rendered)
        temporary_path = Path(temporary.name)
    temporary_path.replace(output)
    history_path = history_dir / f"{generated_at.strftime('%Y%m%dT%H%M%SZ')}.json"
    history_path.write_text(rendered, encoding="utf-8")
    return manifest


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    manifest = write_manifest(
        database_url=settings.database_url,
        output=args.output,
        history_dir=args.history_dir,
    )
    print(
        f"wrote release manifest generated_at={manifest['generated_at'].isoformat()} "
        f"output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
