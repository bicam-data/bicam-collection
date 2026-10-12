"""Classify candidate quality-audit gaps with bounded read-only source checks."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import psycopg

from bicam_collection.config import Settings


GAP_RULES = {
    "bills_actions.calendar_number": ("source_cohort_empty", "119th bill-action staging rows have no calendar number", "recheck future source refreshes"),
    "amendments.notes": ("source_cohort_empty", "119th amendment detail rows have no note rows", "recheck future source refreshes"),
    "amendments_amended_treaties.amendment_id": ("empty_119_relation", "no 119th amendment-to-treaty edges", "retain valid empty relation"),
    "amendments_amended_treaties.treaty_id": ("empty_119_relation", "no 119th amendment-to-treaty edges", "retain valid empty relation"),
    "committee_meetings_associated_treaties.meeting_id": ("empty_119_relation", "no 119th meeting-to-treaty edges", "retain valid empty relation"),
    "committee_meetings_associated_treaties.treaty_id": ("empty_119_relation", "no 119th meeting-to-treaty edges", "retain valid empty relation"),
    "committee_prints_texts.png": ("source_projection_unavailable", "Congress print-text staging exposes no png field", "review GovInfo rendition sources before adding"),
    "committee_reports_associated_treaties.report_id": ("empty_119_relation", "no 119th report-to-treaty edges", "retain valid empty relation"),
    "committee_reports_associated_treaties.treaty_id": ("empty_119_relation", "no 119th report-to-treaty edges", "retain valid empty relation"),
    "congresses_directories_isbn.congress_number": ("source_cohort_empty", "119th directory ISBN relation has no selected row", "check later directory editions"),
    "congresses_directories_isbn.govinfo_package_id": ("source_cohort_empty", "119th directory ISBN relation has no selected row", "check later directory editions"),
    "congresses_directories_isbn.isbn": ("source_cohort_empty", "119th directory ISBN relation has no selected row", "check later directory editions"),
    "members_metadata.gpo_id": ("govinfo_structured_gap", "119th Congressional Directory granule list is empty", "recheck GovInfo granules when exposed"),
    "members_metadata.authority_id": ("govinfo_structured_gap", "119th Congressional Directory granule list is empty", "recheck GovInfo granules when exposed"),
    "members_terms.title": ("govinfo_structured_gap", "119th directory granules do not expose structured granule titles", "do not invent title from prose"),
    "members_terms.twitter_url": ("source_text_unavailable", "captured official directory text has no Twitter links", "recheck a structured official member source"),
    "members_terms.instagram_url": ("source_text_unavailable", "captured official directory text has no Instagram links", "recheck a structured official member source"),
    "members_terms.facebook_url": ("source_text_unavailable", "captured official directory text has no Facebook links", "recheck a structured official member source"),
    "members_terms.youtube_url": ("source_text_unavailable", "captured official directory text has no YouTube links", "recheck a structured official member source"),
    "nominations.executive_calendar_number": ("source_cohort_empty", "119th nomination staging rows have no executive calendar number", "recheck future source refreshes"),
    "nominations_nominees.effective_date": ("source_cohort_empty", "119th nominee staging rows have no effective dates", "recheck future source refreshes"),
    "nominations_nominees.predecessor_name": ("source_cohort_empty", "119th nominee staging rows have no predecessor names", "recheck future source refreshes"),
    "nominations_nominees.corps_code": ("source_cohort_empty", "119th nominee staging rows have no corps codes", "recheck future source refreshes"),
    "treaties.suffix": ("source_payload_null", "selected treaty payload has null suffix", "preserve source NULL"),
    "treaties.congress_considered": ("source_payload_null", "selected treaty payload has null considered Congress", "preserve source NULL"),
    "treaties.in_force_at": ("source_payload_null", "selected treaty payload has null in-force date", "preserve source NULL"),
    "treaties.resolution_text": ("source_payload_null", "selected treaty payload has null resolution text", "preserve source NULL"),
    "treaties.old_number": ("source_payload_null", "selected treaty payload has null old number", "preserve source NULL"),
    "treaties.old_number_display_name": ("source_payload_null", "selected treaty payload has null old number display name", "preserve source NULL"),
}


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def observe(bundle, quality_report, output):
    manifest = json.loads((bundle / "manifest.json").read_text())
    quality = json.loads(quality_report.read_text())
    if quality["candidate_manifest_sha256"] != sha(bundle / "manifest.json"):
        raise ValueError("quality report targets another candidate")
    gaps = [f"{table['table']}.{field}"
            for table in quality["tables"]
            for field, status in table["availability"].items()
            if status["status"] == "candidate_field_gap"]
    if set(gaps) != set(GAP_RULES):
        raise ValueError(f"gap rule inventory differs: missing={sorted(set(gaps)-set(GAP_RULES))}, extra={sorted(set(GAP_RULES)-set(gaps))}")
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    queries = {
        "bill_actions_calendar": """select count(*)::bigint as rows, count(a.calendar_number)::bigint as nonnull_calendar
            from public.stg_congress__bill_actions a join public.stg_congress__bill_detail b using (bill_id) where b.congress=119""",
        "amendment_notes": """select count(distinct a.amendment_id)::bigint as amendments, count(n.note_id)::bigint as notes
            from public.stg_congress__amendment_detail a left join public.stg_congress__amendment_notes n using (amendment_id) where a.congress=119""",
        "committee_print_text_columns": """select string_agg(column_name, ',' order by ordinal_position) as columns
            from information_schema.columns where table_schema='public' and table_name='stg_congress__committee_print_texts'""",
        "nomination_fields": """select count(*)::bigint as nominations, count(executive_calendar_number)::bigint as executive_calendar
            from public.stg_congress__nomination_detail where congress=119""",
        "nominee_fields": """select count(*)::bigint as nominees, count(effective_date)::bigint as effective_date,
            count(predecessor_name)::bigint as predecessor_name, count(corps_code)::bigint as corps_code
            from public.stg_congress__nomination_nominees n join public.stg_congress__nomination_detail d using (nomination_id) where d.congress=119""",
        "treaty_fields": """select count(*)::bigint as treaties, count(suffix)::bigint as suffix,
            count(congress_considered)::bigint as congress_considered, count(in_force_at)::bigint as in_force_at,
            count(resolution_text)::bigint as resolution_text, count(old_number)::bigint as old_number,
            count(old_number_display_name)::bigint as old_number_display_name
            from public.stg_congress__treaty_detail where congress_received=119 or congress_considered=119""",
        "directory_granules": """select count(*)::bigint as granules, count(gpo_id)::bigint as gpo_id,
            count(authority_id)::bigint as authority_id from public.stg_govinfo__congressional_directory_members where congress=119""",
        "treaty_relationships": """select
            (select count(*)::bigint from public.stg_congress__amendment_amended_treaties where congress=119) as amendment_edges,
            (select count(*)::bigint from public.stg_congress__committee_meeting_treaties where congress=119) as meeting_edges,
            (select count(*)::bigint from public.stg_congress__treaty_committeereports where congress=119) as report_reverse_edges""",
    }
    observations = {}
    with psycopg.connect(settings.database_url, options=(
        "-c default_transaction_read_only=on -c statement_timeout=60000 "
        "-c lock_timeout=1000 -c max_parallel_workers_per_gather=0"
    )) as connection:
        for name, query in queries.items():
            print(json.dumps({"query": name}), flush=True)
            cursor = connection.execute(query)
            row = cursor.fetchone()
            observations[name] = {description: value for description, value in zip(
                [column.name for column in cursor.description], row)}
    zero_observations = {
        "bill_actions_calendar": ("nonnull_calendar",),
        "amendment_notes": ("notes",),
        "nomination_fields": ("executive_calendar",),
        "nominee_fields": ("effective_date", "predecessor_name", "corps_code"),
        "treaty_fields": ("suffix", "congress_considered", "in_force_at", "resolution_text", "old_number", "old_number_display_name"),
        "directory_granules": ("granules", "gpo_id", "authority_id"),
        "treaty_relationships": ("amendment_edges", "meeting_edges", "report_reverse_edges"),
    }
    for query_name, fields in zero_observations.items():
        for field in fields:
            value = observations[query_name][field]
            if value != 0:
                raise ValueError(f"expected zero observation for {query_name}.{field}, got {value}")
    if "png" in (observations["committee_print_text_columns"]["columns"] or "").split(","):
        raise ValueError("committee print staging unexpectedly exposes png")
    decisions = [{"gap": gap, "classification": GAP_RULES[gap][0], "evidence": GAP_RULES[gap][1],
                  "next_action": GAP_RULES[gap][2]} for gap in sorted(gaps)]
    result = {"schema_version": 1, "observed_at": datetime.now(timezone.utc).isoformat(),
              "candidate_manifest_sha256": sha(bundle / "manifest.json"),
              "quality_report_sha256": sha(quality_report), "database_snapshot_at": manifest.get("database_snapshot_at"),
              "query_observations": observations, "decisions": decisions,
              "production_modified": False, "release_accepted": False,
              "limitations": ["Staging observations were queried after candidate creation and are evidence for source shape, not a new export.",
                              "Empty/current observations do not prove future source absence; every decision has a refresh action."]}
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as stream:
        json.dump(result, stream, indent=2, default=str)
        stream.write("\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--quality-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = observe(args.bundle, args.quality_report, args.output)
    print(json.dumps({"gaps": len(result["decisions"]), "query_observations": len(result["query_observations"])}))
