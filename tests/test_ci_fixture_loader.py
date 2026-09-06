from __future__ import annotations

import importlib.util
from pathlib import Path


def _fixture_rows() -> list[tuple[str, str, str, str, dict, str | None]]:
    project_root = Path(__file__).resolve().parents[1]
    module_path = project_root / "scripts" / "load_ci_fixtures.py"
    spec = importlib.util.spec_from_file_location("load_ci_fixtures", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._fixture_rows()


def test_ci_fixture_corpus_exercises_bill_text_child_lineage() -> None:
    rows = _fixture_rows()
    bill_details = {
        endpoint
        for endpoint, _source, _category, node_type, _response, _parent in rows
        if node_type == "bill_detail"
    }
    member_details = [row for row in rows if row[3] == "member_detail"]
    member_sponsored = [
        row for row in rows if row[3] == "member_sponsored_legislation"
    ]
    member_cosponsored = [
        row for row in rows if row[3] == "member_cosponsored_legislation"
    ]
    bill_text_rows = [
        row for row in rows if row[3] == "bill_text"
    ]

    assert len(rows) == 42
    assert len(member_details) == 1
    assert len(member_sponsored) == 1
    assert len(member_cosponsored) == 1
    assert member_sponsored[0][5] == member_details[0][0]
    assert member_cosponsored[0][5] == member_details[0][0]
    assert len(member_sponsored[0][4]["sponsoredLegislation"]) == 4
    assert len(member_cosponsored[0][4]["cosponsoredLegislation"]) == 4
    assert len(bill_text_rows) == 1
    assert bill_text_rows[0][5] in bill_details
    assert bill_text_rows[0][4]["textVersions"][0]["formats"][0]["url"].endswith(
        "BILLS-118hr1ih.htm"
    )
    committee_report_details = [row for row in rows if row[3] == "committee_report_detail"]
    committee_report_texts = [row for row in rows if row[3] == "committee_report_texts"]
    assert len(committee_report_details) == 1
    assert len(committee_report_texts) == 2
    assert all(row[5] == committee_report_details[0][0] for row in committee_report_texts)
    assert committee_report_texts[0][4]["text"][0]["formats"][0]["type"] == "Formatted Text"
    assert committee_report_texts[1][4]["text"][0]["formats"][0]["isErrata"] == "Y"
    bill_amendments = [row for row in rows if row[3] == "bill_amendments"]
    assert len(bill_amendments) == 1
    assert bill_amendments[0][5] in bill_details
    assert len(bill_amendments[0][4]["amendments"]) == 2
    bill_committees = [row for row in rows if row[3] == "bill_committees"]
    assert len(bill_committees) == 1
    assert len(bill_committees[0][4]["committees"]) == 2
    subjects = [row for row in rows if row[3] == "bill_subjects"]
    summaries = [row for row in rows if row[3] == "bill_summaries"]
    assert len(subjects) == 3
    assert len(summaries) == 1
    assert all(row[5] in bill_details for row in subjects)
    assert summaries[0][5] in bill_details
    assert any("legislativeSubjects" in row[4]["subjects"] for row in subjects)
    source_removal_subjects = [
        row for row in subjects if "/bill/118/hr/2/subjects" in row[0]
    ]
    assert len(source_removal_subjects) == 2
    assert any(row[4]["subjects"] for row in source_removal_subjects)
    assert any(row[4]["subjects"] == [] for row in source_removal_subjects)

    bill_actions = [row for row in rows if row[3] == "bill_actions"]
    assert len(bill_actions) == 1
    assert bill_actions[0][5] in bill_details
    assert len(bill_actions[0][4]["actions"]) == 3
    assert [vote["rollNumber"] for vote in bill_actions[0][4]["actions"][0]["recordedVotes"]] == [183, 184]
    assert [committee["systemCode"] for committee in bill_actions[0][4]["actions"][0]["committees"]] == ["HSAG", "HSAG"]

    related_bill_rows = [row for row in rows if row[3] == "bill_relatedbills"]
    assert len(related_bill_rows) == 1
    assert related_bill_rows[0][5] in bill_details
    assert related_bill_rows[0][4]["relatedBills"][0]["number"] == "2"

    committee_details = [row for row in rows if row[3] == "committee_detail"]
    committee_bills = [row for row in rows if row[3] == "committee_bills"]
    committee_reports = [row for row in rows if row[3] == "committee_reports"]
    committee_nominations = [row for row in rows if row[3] == "committee_nominations"]
    committee_communications = [row for row in rows if row[3] == "committee_communications"]
    bill_committees = [row for row in rows if row[3] == "bill_committees"]
    assert len(committee_details) == 1
    assert len(committee_bills) == 1
    assert len(committee_reports) == 1
    assert len(committee_nominations) == 1
    assert len(committee_communications) == 1
    assert len(bill_committees) == 1
    assert committee_bills[0][5] in {
        row[0] for row in committee_details
    }
    assert bill_committees[0][5] in bill_details
    assert committee_reports[0][5] in {
        row[0] for row in committee_details
    }
    assert len(committee_reports[0][4]["reports"]) == 2
    assert len(committee_nominations[0][4]["nominations"]) == 2
    assert len(committee_communications[0][4]["houseCommunications"]) == 2

    amendment_details = {
        endpoint
        for endpoint, _source, _category, node_type, _response, _parent in rows
        if node_type == "amendment_detail"
    }
    amendment_actions = [row for row in rows if row[3] == "amendment_actions"]
    assert len(amendment_details) == 1
    assert len(amendment_actions) == 1
    assert amendment_actions[0][5] in amendment_details
    assert len(amendment_actions[0][4]["actions"]) == 3
    amendment_texts = [row for row in rows if row[3] == "amendment_texts"]
    assert len(amendment_texts) == 1
    assert amendment_texts[0][5] in amendment_details
    assert len(amendment_texts[0][4]["textVersions"]) == 2
    assert amendment_texts[0][4]["textVersions"][0]["formats"][1]["type"] == "PDF"
    amendment_amendments = [
        row for row in rows if row[3] == "amendment_amendments_to_amendment"
    ]
    assert len(amendment_amendments) == 1
    assert amendment_amendments[0][5] in amendment_details
    assert len(amendment_amendments[0][4]["amendments"]) == 2
    assert [vote["rollNumber"] for vote in amendment_actions[0][4]["actions"][0]["recordedVotes"]] == [354, 355]
    amendment_cosponsors = [row for row in rows if row[3] == "amendment_cosponsors"]
    assert len(amendment_cosponsors) == 2
    assert all(row[5] in amendment_details for row in amendment_cosponsors)
    nomination_details = [row for row in rows if row[3] == "nomination_detail"]
    nomination_actions = [row for row in rows if row[3] == "nomination_actions"]
    assert len(nomination_details) == 1
    nomination_nominees = [row for row in rows if row[3] == "nomination_nominee_detail"]
    assert len(nomination_nominees) == 1
    assert nomination_nominees[0][5] == nomination_details[0][0]
    assert len(nomination_nominees[0][4]["nominees"]) == 2
    assert {nominee["lastName"] for nominee in nomination_nominees[0][4]["nominees"]} == {"Alpha", "Beta"}
    assert len(nomination_actions) == 1
    assert nomination_actions[0][5] == nomination_details[0][0]
    assert len(nomination_actions[0][4]["actions"]) == 2
    assert [committee["systemCode"] for committee in nomination_actions[0][4]["actions"][0]["committees"]] == ["HSAG", "HSAG"]
    nomination_hearings = [row for row in rows if row[3] == "nomination_hearings"]
    nomination_committees = [row for row in rows if row[3] == "nomination_committees"]
    assert len(nomination_hearings) == 1
    assert len(nomination_committees) == 1
    assert len(nomination_hearings[0][4]["hearings"]) == 2
    assert len(nomination_committees[0][4]["committees"][0]["activities"]) == 2
    treaty_details = [row for row in rows if row[3] == "treaty_detail"]
    treaty_committees = [row for row in rows if row[3] == "treaty_committees"]
    assert len(treaty_details) == 1
    assert len(treaty_committees) == 1
    assert treaty_committees[0][5] == treaty_details[0][0]
    assert len(treaty_committees[0][4]["treatyCommittees"]) == 2
    treaty_actions = [row for row in rows if row[3] == "treaty_actions"]
    assert len(treaty_actions) == 1
    assert treaty_actions[0][5] == treaty_details[0][0]
    assert len(treaty_actions[0][4]["actions"]) == 2
    committee_print_details = [row for row in rows if row[3] == "committee_print_detail"]
    committee_print_texts = [row for row in rows if row[3] == "committee_print_texts"]
    assert len(committee_print_details) == 1
    assert len(committee_print_texts) == 1
    assert committee_print_texts[0][5] == committee_print_details[0][0]
    assert len(committee_print_texts[0][4]["text"][0]["formats"]) == 3
    assert committee_print_texts[0][4]["text"][1]["type"] == "Formatted Text"


def test_ci_fixture_corpus_covers_adversarial_release_cases() -> None:
    """Keep Package-A/C/G's non-sparse cases in the disposable corpus.

    The assertions distinguish a repeated array member from a repeated fetch,
    and preserve negative controls that must remain separate.  They are
    intentionally fixture-level checks; the dbt selector exercises their
    actual public reductions separately.
    """

    rows = _fixture_rows()
    by_type = {}
    for row in rows:
        by_type.setdefault(row[3], []).append(row)

    # Duplicate observations inside one authoritative payload.
    sponsored = by_type["member_sponsored_legislation"][0][4]["sponsoredLegislation"]
    assert sponsored[0] == sponsored[1]
    assert sponsored[2] == sponsored[3]

    # The same observation repeated across two differently parameterized
    # fetches has two transport rows but identical source content.
    cosponsor_rows = by_type["amendment_cosponsors"]
    assert len(cosponsor_rows) == 2
    assert cosponsor_rows[0][0].split("?")[0] == cosponsor_rows[1][0].split("?")[0]
    assert cosponsor_rows[0][4] == cosponsor_rows[1][4]

    # Negative controls: same-position nominees and distinct bill IDs survive
    # as separate source records rather than being merged by ordinal/name.
    nominees = by_type["nomination_nominee_detail"][0][4]["nominees"]
    assert [nominee["ordinal"] for nominee in nominees] == [1, 1]
    assert {nominee["lastName"] for nominee in nominees} == {"Alpha", "Beta"}
    bill_ids = {
        row[4]["bill"]["type"].lower() + row[4]["bill"]["number"] + "-" + str(row[4]["bill"]["congress"])
        for row in by_type["bill_detail"]
    }
    assert {"hr1-118", "hr2-118"}.issubset(bill_ids)

    # One action carries two roll calls; both are required at the public vote
    # grain even though their surrounding action observations repeat.
    bill_action = by_type["bill_actions"][0][4]["actions"][0]
    assert [vote["rollNumber"] for vote in bill_action["recordedVotes"]] == [183, 184]

    # The report text fixtures represent a baseline plus a later date-window
    # delta, not two unrelated collections.
    report_texts = by_type["committee_report_texts"]
    assert len(report_texts) == 2
    assert {row[5] for row in report_texts} == {by_type["committee_report_detail"][0][0]}

    # A complete empty child generation must retire the earlier source row;
    # the two transport URLs intentionally normalize to one collection.
    source_removal_subjects = [
        row for row in by_type["bill_subjects"] if "/bill/118/hr/2/subjects" in row[0]
    ]
    assert len(source_removal_subjects) == 2
    assert any(row[4]["subjects"] for row in source_removal_subjects)
    assert any(row[4]["subjects"] == [] for row in source_removal_subjects)
