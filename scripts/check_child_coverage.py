"""Report missing stored responses for declared Congress child edges.

The root-detail coverage check cannot detect a detail payload whose paginated
children were never harvested.  This read-only audit expands each declared
Congress child URL from the current raw payloads, normalizes it to an endpoint
path, and compares it with the immutable current-response archive.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import tempfile

import psycopg
import yaml

from bicam_collection.config import Settings
from bicam_collection.target_scope import TARGET_CONGRESS_CATEGORIES


def _configure_audit_connection(connection: psycopg.Connection) -> None:
    """Bound audit work and avoid orphaned parallel workers on cancellation.

    The supervisor may terminate an audit shell while a JSON-expansion query is
    still running.  PostgreSQL parallel workers can outlive that client long
    enough to consume CPU, so this diagnostic deliberately uses one backend;
    the indexed temporary response-path table still keeps the joins bounded.
    """

    connection.execute("set statement_timeout='120s'")
    connection.execute("set lock_timeout='15s'")
    connection.execute("set max_parallel_workers_per_gather=0")


def _edges(project_root: Path) -> list[tuple[str, str, str, str, str | None, str | None]]:
    edges: list[tuple[str, str, str, str, str | None, str | None]] = []
    for path in sorted((project_root / "specs").glob("*.yaml")):
        spec = yaml.safe_load(path.read_text(encoding="utf-8"))
        if spec.get("source") != "congress":
            continue
        category = str(spec["category"])
        if category not in TARGET_CONGRESS_CATEGORIES:
            continue
        for parent_node, node in (spec.get("nodes") or {}).items():
            if parent_node.endswith("_list"):
                continue
            for edge in node.get("child_edges") or []:
                edges.append(
                    (
                        category,
                        str(parent_node),
                        str(edge["target"]),
                        str(edge["url_path"]),
                        edge.get("fallback_url_suffix"),
                        edge.get("url_normalizer"),
                    )
                )
    return edges


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fail-on-missing",
        action="store_true",
        help="Return exit status 1 when any declared child response is missing.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write a timestamped checkpoint plus a latest copy to this path.",
    )
    parser.add_argument(
        "--history-dir",
        type=Path,
        default=None,
        help="Directory for the timestamped checkpoint (defaults to output's directory).",
    )
    return parser.parse_args(argv)


def _paginated_item_coverage(connection: psycopg.Connection) -> dict[str, int] | None:
    """Measure item-level coverage for paginated child collections.

    Endpoint-path coverage is sufficient for non-paginated children, but it
    can hide one poisoned row in an otherwise recoverable page.  Congress.gov
    currently exposes this failure mode on bill cosponsors: a 250-row page
    returns HTTP 500 when one historical member term is malformed, while
    smaller pages return every other row.  Count distinct returned items
    against the source-reported pagination count and classify only the
    shortfall on a repeated source-error path as an upstream gap.

    Keep this query deliberately scoped to the known paginated child shape;
    additional shapes can be added as their APIs demonstrate the same
    behavior.  The endpoint-level audit remains the authoritative check for
    whether a child response exists at all.
    """

    row = connection.execute(
        """
        with source_errors as (
            select
                regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
            from failed_requests
            where category = 'bills'
              and node_type = 'bill_cosponsors'
              and http_status = 500
              and error like '%%source_error=%%'
            group by 1
            having count(*) >= 3
        ),
        expected as (
            select
                regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path,
                max((response -> 'pagination' ->> 'count')::bigint) as expected_items
            from raw_responses
            join source_errors
              on source_errors.endpoint_path = regexp_replace(endpoint_url, '[?].*$', '')
            where source = 'congress'
              and category = 'bills'
              and node_type = 'bill_cosponsors'
              and response ? 'pagination'
            group by 1
        ),
        fetched as (
            select
                regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path,
                count(
                    distinct coalesce(
                        item ->> 'bioguideId',
                        item ->> 'bioguideID',
                        item ->> 'fullName',
                        item #>> '{}'
                    )
                ) as fetched_items
            from raw_responses
            cross join lateral jsonb_array_elements(
                coalesce(response -> 'cosponsors', '[]'::jsonb)
            ) as items(item)
            join source_errors
              on source_errors.endpoint_path = regexp_replace(endpoint_url, '[?].*$', '')
            where source = 'congress'
              and category = 'bills'
              and node_type = 'bill_cosponsors'
            group by 1
        ),
        measured as (
            select
                expected.expected_items,
                coalesce(fetched.fetched_items, 0) as fetched_items,
                source_errors.endpoint_path is not null as has_source_error
            from expected
            left join fetched using (endpoint_path)
            left join source_errors using (endpoint_path)
        )
        select
            coalesce(sum(expected_items), 0)::bigint as expected_items,
            coalesce(sum(fetched_items), 0)::bigint as fetched_items,
            coalesce(sum(
                case
                    when has_source_error and expected_items > fetched_items
                    then expected_items - fetched_items
                    else 0
                end
            ), 0)::bigint as source_gap_items,
            coalesce(sum(
                case
                    when not has_source_error and expected_items > fetched_items
                    then expected_items - fetched_items
                    else 0
                end
            ), 0)::bigint as missing_items
        from measured
        """
    ).fetchone()
    if not row:
        return None
    return {
        "expected": int(row[0] or 0),
        "fetched": int(row[1] or 0),
        "source_gaps": int(row[2] or 0),
        "missing": int(row[3] or 0),
    }


def _nomination_nominee_item_coverage(
    connection: psycopg.Connection,
) -> dict[str, int] | None:
    """Measure complete pagination for each nomination nominee group.

    A nomination detail advertises one URL per nominee group/position, while
    that URL is itself paginated. Merely observing the first response therefore
    does not prove that every named nominee was harvested.
    """

    row = connection.execute(
        """
        with expected as (
            select
                regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path,
                max((response -> 'pagination' ->> 'count')::bigint) as expected_items
            from raw_responses
            where source = 'congress'
              and category = 'nominations'
              and node_type = 'nomination_nominee_detail'
              and response -> 'pagination' ->> 'count' is not null
            group by 1
        ),
        fetched as (
            select
                regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path,
                /* ordinal identifies a position/group, not an individual
                   nominee. Multiple people can legitimately share it, so
                   count every source observation when auditing pagination. */
                count(*) as fetched_items
            from raw_responses
            cross join lateral jsonb_array_elements(
                coalesce(response -> 'nominees', '[]'::jsonb)
            ) as items(item)
            where source = 'congress'
              and category = 'nominations'
              and node_type = 'nomination_nominee_detail'
            group by 1
        ),
        source_empty as (
            /* A few historical nominee groups now return a valid empty
               response (pagination.count=0) even though an earlier source
               observation advertised rows. Treat that as an observed source
               gap rather than an actionable crawler omission. */
            select distinct regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
            from raw_responses
            where source = 'congress'
              and category = 'nominations'
              and node_type = 'nomination_nominee_detail'
              and response -> 'pagination' ->> 'count' = '0'
              and jsonb_array_length(coalesce(response -> 'nominees', '[]'::jsonb)) = 0
        ),
        measured as (
            select
                expected.expected_items,
                coalesce(fetched.fetched_items, 0) as fetched_items,
                (errors.endpoint_path is not null
                 or source_empty.endpoint_path is not null) as has_source_error
            from expected
            left join fetched using (endpoint_path)
            left join child_coverage_source_errors as errors using (endpoint_path)
            left join source_empty using (endpoint_path)
        )
        select
            coalesce(sum(expected_items), 0)::bigint as expected_items,
            coalesce(sum(fetched_items), 0)::bigint as fetched_items,
            coalesce(sum(
                case
                    when has_source_error and expected_items > fetched_items
                    then expected_items - fetched_items
                    else 0
                end
            ), 0)::bigint as source_gap_items,
            coalesce(sum(
                case
                    when not has_source_error and expected_items > fetched_items
                    then expected_items - fetched_items
                    else 0
                end
            ), 0)::bigint as missing_items
        from measured
        """
    ).fetchone()
    if not row:
        return None
    return {
        "expected": int(row[0] or 0),
        "fetched": int(row[1] or 0),
        "source_gaps": int(row[2] or 0),
        "missing": int(row[3] or 0),
    }


def _collection_item_coverage(
    connection: psycopg.Connection,
    *,
    category: str,
    node_type: str,
    array_key: str,
) -> dict[str, int] | None:
    """Compare source pagination counts with observed child rows.

    Public marts may intentionally deduplicate repeated source objects. The
    completeness check therefore uses observed rows, not distinct JSON hashes,
    so duplicate-heavy pages do not become a false API backlog.
    """

    row = connection.execute(
        """
        with expected as (
            select
                regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path,
                max((response -> 'pagination' ->> 'count')::bigint) as expected_items
            from raw_responses
            where source = 'congress'
              and category = %s
              and node_type = %s
              and response -> 'pagination' ->> 'count' is not null
            group by 1
        ),
        fetched as (
            select
                regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path,
                count(*) as observed_items,
                count(distinct md5(item::text)) as distinct_items
            from raw_responses
            cross join lateral jsonb_array_elements(
                coalesce(response -> %s, '[]'::jsonb)
            ) as items(item)
            where source = 'congress'
              and category = %s
              and node_type = %s
            group by 1
        ),
        measured as (
            select
                expected.expected_items,
                coalesce(fetched.observed_items, 0) as observed_items,
                errors.endpoint_path is not null as has_source_error
            from expected
            left join fetched using (endpoint_path)
            left join child_coverage_source_errors as errors using (endpoint_path)
        )
        select
            coalesce(sum(expected_items), 0)::bigint,
            coalesce(sum(observed_items), 0)::bigint,
            coalesce(sum(
                case when has_source_error and expected_items > observed_items
                    then expected_items - observed_items else 0 end
            ), 0)::bigint,
            coalesce(sum(
                case when not has_source_error and expected_items > observed_items
                    then expected_items - observed_items else 0 end
            ), 0)::bigint
        from measured
        """,
        (category, node_type, array_key, category, node_type),
    ).fetchone()
    if not row:
        return None
    return {
        "expected": int(row[0] or 0),
        "fetched": int(row[1] or 0),
        "source_gaps": int(row[2] or 0),
        "missing": int(row[3] or 0),
    }


def _coverage_totals(results: list[dict[str, object]]) -> dict[str, int]:
    endpoint_results = [
        result for result in results if result.get("coverage_level", "endpoint") == "endpoint"
    ]
    item_results = [result for result in results if result.get("coverage_level") == "item"]
    endpoint_missing = sum(int(result["missing"]) for result in endpoint_results)
    item_missing = sum(int(result["missing"]) for result in item_results)
    return {
        "endpoint_expected": sum(int(result["expected"]) for result in endpoint_results),
        "endpoint_source_gaps": sum(int(result["source_gaps"]) for result in endpoint_results),
        "endpoint_missing": endpoint_missing,
        "item_expected": sum(int(result["expected"]) for result in item_results),
        "item_fetched": sum(int(result["fetched"]) for result in item_results),
        "item_source_gaps": sum(int(result["source_gaps"]) for result in item_results),
        "item_missing": item_missing,
        "missing": endpoint_missing + item_missing,
    }


def run(*, database_url: str, project_root: Path) -> list[dict[str, object]]:
    edges = _edges(project_root)
    results: list[dict[str, object]] = []
    with psycopg.connect(database_url) as connection:
        _configure_audit_connection(connection)
        connection.execute(
            """
            create temporary table child_coverage_detail_paths as
            select distinct regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
            from raw_responses
            where source = 'congress'
            """
        )
        connection.execute(
            """
            create index child_coverage_detail_paths_idx
            on child_coverage_detail_paths(endpoint_path)
            """
        )
        # A child URL can be declared by a parent while Congress.gov itself
        # returns a durable 404 (for example a committee meeting's stale
        # hearingTranscript link).  Keep those failures in the ledger, but do
        # not report them as actionable crawler omissions.  A 500 needs three
        # observations before it is treated as source-side, matching the root
        # detail quality gate; a 404 is definitive on its first attempt.
        connection.execute(
            """
            create temporary table child_coverage_source_errors as
            select regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
            from failed_requests
            where http_status = 404
            group by regexp_replace(endpoint_url, '[?].*$', '')
            union
            select regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
            from failed_requests
            where http_status = 500
            group by regexp_replace(endpoint_url, '[?].*$', '')
            having count(*) >= 3
            """
        )
        connection.execute(
            """
            create index child_coverage_source_errors_idx
            on child_coverage_source_errors(endpoint_path)
            """
        )
        connection.execute("analyze child_coverage_source_errors")
        # The temporary table has no inherited statistics.  Without ANALYZE,
        # PostgreSQL often chooses repeated nested-loop scans for the edge
        # joins once the raw archive grows into the millions of rows.
        connection.execute("analyze child_coverage_detail_paths")
        connection.commit()
        for category, parent_node, target_node, url_path, fallback_suffix, url_normalizer in edges:
            # Mirror crawler semantics exactly: prefer URLs advertised in the
            # payload; construct a suffix fallback only for parents that omit
            # the advertised URL; finally apply any declarative edge URL
            # normalization before testing coverage.
            query = """
                with parents as (
                    select id, endpoint_url, response
                    from raw_responses
                    where source = 'congress'
                      and category = %s
                      and node_type = %s
                ),
                advertised as (
                    select distinct
                        parent.id as parent_id,
                        regexp_replace(value #>> '{}', '[?].*$', '') as endpoint_path
                    from parents as parent
                    cross join lateral jsonb_path_query(
                        parent.response, %s::jsonpath
                    ) as value
                    where jsonb_typeof(value) = 'string'
                      and value #>> '{}' like 'http%%'
                ),
                fallback as (
                    select
                        parent.id as parent_id,
                        regexp_replace(parent.endpoint_url, '[?].*$', '')
                        || '/' || %s as endpoint_path
                    from parents as parent
                    where %s::text is not null
                      and not exists (
                          select 1 from advertised
                          where advertised.parent_id = parent.id
                      )
                ),
                expected_raw as (
                    select endpoint_path from advertised
                    union all
                    select endpoint_path from fallback
                ),
                expected as (
                    select distinct
                        case
                            when %s = 'congress_treaty_committees' then regexp_replace(
                                endpoint_path,
                                '/committees/?$',
                                '/committees'
                            )
                            else endpoint_path
                        end as endpoint_path
                    from expected_raw
                )
                select count(*),
                       count(*) filter (where paths.endpoint_path is not null),
                       count(*) filter (
                           where paths.endpoint_path is null
                             and errors.endpoint_path is not null
                       )
                from expected
                left join child_coverage_detail_paths as paths using (endpoint_path)
                left join child_coverage_source_errors as errors using (endpoint_path)
            """
            normalized_fallback = fallback_suffix.strip("/") if fallback_suffix else None
            params = (
                category,
                parent_node,
                url_path,
                normalized_fallback,
                normalized_fallback,
                url_normalizer,
            )
            try:
                expected, fetched, source_gaps = connection.execute(query, params).fetchone()
            except Exception:
                connection.rollback()
                raise
            expected = int(expected or 0)
            fetched = int(fetched or 0)
            source_gaps = int(source_gaps or 0)
            results.append(
                {
                    "category": category,
                    "parent_node": parent_node,
                    "target_node": target_node,
                    "expected": expected,
                    "fetched": fetched,
                    "source_gaps": source_gaps,
                    "missing": expected - fetched - source_gaps,
                }
            )
        paginated_items = _paginated_item_coverage(connection)
        if paginated_items is not None:
            results.append(
                {
                    "coverage_level": "item",
                    "category": "bills",
                    "parent_node": "bill_detail",
                    "target_node": "bill_cosponsors",
                    **paginated_items,
                }
            )
        nomination_nominee_items = _nomination_nominee_item_coverage(connection)
        if nomination_nominee_items is not None:
            results.append(
                {
                    "coverage_level": "item",
                    "category": "nominations",
                    "parent_node": "nomination_detail",
                    "target_node": "nomination_nominee_detail",
                    **nomination_nominee_items,
                }
            )
        for category, parent_node, target_node, array_key in (
            ("amendments", "amendment_detail", "amendment_actions", "actions"),
            ("amendments", "amendment_detail", "amendment_texts", "textVersions"),
            ("bills", "bill_detail", "bill_actions", "actions"),
            ("bills", "bill_detail", "bill_text", "textVersions"),
            (
                "committee-reports",
                "committee_report_detail",
                "committee_report_texts",
                "text",
            ),
        ):
            collection_items = _collection_item_coverage(
                connection,
                category=category,
                node_type=target_node,
                array_key=array_key,
            )
            if collection_items is not None:
                results.append(
                    {
                        "coverage_level": "item",
                        "category": category,
                        "parent_node": parent_node,
                        "target_node": target_node,
                        **collection_items,
                    }
                )
        # Committee meetings and hearings are separate Congress.gov specs, so
        # their transcript edge cannot be declared as an in-spec child edge.
        # Keep it in the same audit: a meeting may reference a hearing, while
        # many standalone hearings have no meeting at all.
        expected, fetched, source_gaps = connection.execute(
            """
            with expected as (
                select distinct regexp_replace(item->>'url', '[?].*$', '') as endpoint_path
                from raw_responses as parent
                cross join lateral jsonb_array_elements(
                    coalesce(parent.response->'committeeMeeting'->'hearingTranscript', '[]'::jsonb)
                ) as item
                where parent.source = 'congress'
                  and parent.category = 'committee-meetings'
                  and parent.node_type = 'committee_meeting_detail'
                  and item->>'url' is not null
            )
            select count(*),
                   count(*) filter (where paths.endpoint_path is not null),
                   count(*) filter (
                       where paths.endpoint_path is null
                         and errors.endpoint_path is not null
                   )
            from expected
            left join child_coverage_detail_paths as paths using (endpoint_path)
            left join child_coverage_source_errors as errors using (endpoint_path)
            """
        ).fetchone()
        expected = int(expected or 0)
        fetched = int(fetched or 0)
        source_gaps = int(source_gaps or 0)
        results.append(
            {
                "category": "committee-meetings",
                "parent_node": "committee_meeting_detail",
                "target_node": "hearings/hearing_detail",
                "expected": expected,
                "fetched": fetched,
                "source_gaps": source_gaps,
                "missing": expected - fetched - source_gaps,
            }
        )
        # The reverse edge is independently advertised by hearing details.
        # A hearing can identify an associated committee meeting even when the
        # meeting-side payload has no transcript link, so both directions must
        # be harvested and audited without assuming one-to-one equivalence.
        expected, fetched, source_gaps = connection.execute(
            """
            with expected as (
                select distinct regexp_replace(
                    parent.response->'hearing'->'associatedMeeting'->>'url',
                    '[?].*$',
                    ''
                ) as endpoint_path
                from raw_responses as parent
                where parent.source = 'congress'
                  and parent.category = 'hearings'
                  and parent.node_type = 'hearing_detail'
                  and parent.response->'hearing'->'associatedMeeting'->>'url' is not null
            )
            select count(*),
                   count(*) filter (where paths.endpoint_path is not null),
                   count(*) filter (
                       where paths.endpoint_path is null
                         and errors.endpoint_path is not null
                   )
            from expected
            left join child_coverage_detail_paths as paths using (endpoint_path)
            left join child_coverage_source_errors as errors using (endpoint_path)
            """
        ).fetchone()
        expected = int(expected or 0)
        fetched = int(fetched or 0)
        source_gaps = int(source_gaps or 0)
        results.append(
            {
                "category": "hearings",
                "parent_node": "hearing_detail",
                "target_node": "committee-meetings/committee_meeting_detail",
                "expected": expected,
                "fetched": fetched,
                "source_gaps": source_gaps,
                "missing": expected - fetched - source_gaps,
            }
        )
    return results


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    results = run(database_url=settings.database_url, project_root=project_root)
    endpoint_results = [
        result for result in results if result.get("coverage_level", "endpoint") == "endpoint"
    ]
    item_results = [
        result for result in results if result.get("coverage_level") == "item"
    ]
    totals = _coverage_totals(results)
    lines: list[str] = []
    for result in endpoint_results:
        missing = int(result["missing"])
        lines.append(
            f"{result['category']} {result['parent_node']} -> {result['target_node']} "
            f"expected={result['expected']} fetched={result['fetched']} "
            f"source_gaps={result['source_gaps']} missing={missing}"
        )
    for result in item_results:
        missing = int(result["missing"])
        lines.append(
            f"{result['category']} {result['parent_node']} -> {result['target_node']} "
            f"coverage=item expected={result['expected']} fetched={result['fetched']} "
            f"source_gaps={result['source_gaps']} missing={missing}"
        )
    lines.append(f"child_coverage_expected_total={totals['endpoint_expected']}")
    lines.append(f"child_coverage_source_gaps_total={totals['endpoint_source_gaps']}")
    lines.append(f"child_coverage_endpoint_missing_total={totals['endpoint_missing']}")
    lines.append(f"child_coverage_missing_total={totals['missing']}")
    if item_results:
        lines.append(
            "child_coverage_pagination_expected_items_total="
            + str(totals["item_expected"])
        )
        lines.append(
            "child_coverage_pagination_fetched_items_total="
            + str(totals["item_fetched"])
        )
        lines.append(
            "child_coverage_pagination_source_gap_items_total="
            + str(totals["item_source_gaps"])
        )
        lines.append(
            "child_coverage_pagination_missing_items_total="
            + str(totals["item_missing"])
        )
    body = "\n".join(lines) + "\n"
    if args.output is not None:
        generated_at = datetime.now(timezone.utc)
        report = (
            "Child-coverage audit checkpoint: "
            + generated_at.strftime("%Y-%m-%d %H:%M UTC")
            + "\n\n"
            + body
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        history_dir = args.history_dir or args.output.parent
        history_dir.mkdir(parents=True, exist_ok=True)
        history_path = history_dir / (
            "child-coverage-audit-" + generated_at.strftime("%Y%m%dT%H%M%SZ") + ".txt"
        )
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=history_dir, delete=False
        ) as temporary:
            temporary.write(report)
            temporary_path = Path(temporary.name)
        temporary_path.replace(history_path)
        # The long-lived latest path is normally a symlink to the newest
        # timestamped artifact.  Never write through an old symlink: doing so
        # would mutate historical evidence while leaving latest pointed at a
        # stale filename. Replace the link atomically after the new artifact
        # is durable.
        if args.output.is_symlink() or args.output.name.endswith("-latest.txt"):
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", dir=args.output.parent, delete=False
            ) as temporary_link:
                temporary_link_path = Path(temporary_link.name)
            temporary_link_path.unlink()
            temporary_link_path.symlink_to(history_path)
            temporary_link_path.replace(args.output)
        else:
            args.output.write_text(report, encoding="utf-8")
        print(report, end="")
    else:
        print(body, end="")
    return 1 if args.fail_on_missing and totals["missing"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
