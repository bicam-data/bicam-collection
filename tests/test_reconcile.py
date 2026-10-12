from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import reconcile  # noqa: E402
from scripts.reconcile import QUEUES, DecisionRequest, build_seed_row, read_worksheet, upsert_seed_row  # noqa: E402

NOW = datetime(2026, 10, 12, 17, 30, tzinfo=timezone.utc)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _request(**overrides: object) -> DecisionRequest:
    fields = {"key": {"entity": "bill", "canonical_id": "hr1-119"}, "decision": "out_of_scope",
              "value": None, "reason": "Not a bill.", "expires_at": None}
    fields.update(overrides)
    return DecisionRequest(**fields)  # type: ignore[arg-type]


def test_every_decision_targets_a_real_seed_with_the_columns_it_writes() -> None:
    for queue in QUEUES.values():
        for decision in queue.decisions.values():
            header = (reconcile.SEED_DIR / f"{decision.seed}.csv").read_text(encoding="utf-8").splitlines()[0].split(",")
            written = {*queue.keys, *queue.context, *decision.fixed, "reviewer", "reviewed_at", "reason", "expires_at"}
            written |= {column for column in (decision.decision_column, decision.value_column) if column}
            assert written <= set(header), (queue.name, decision.seed, written - set(header))


def test_every_decision_seed_has_pinned_column_types() -> None:
    # An unpinned empty seed is created with all-integer columns, which then
    # rejects the first text decision written to it.
    config = yaml.safe_load((PROJECT_ROOT / "dbt_project" / "dbt_project.yml").read_text(encoding="utf-8"))
    pinned = config["seeds"]["bicam_collection"]["reconciliation"]
    for queue in QUEUES.values():
        for decision in queue.decisions.values():
            assert "+column_types" in pinned.get(decision.seed, {}), decision.seed


def test_disposition_row_uses_timestamps_and_contract_version() -> None:
    seed, row = build_seed_row(QUEUES["match-exceptions"], _request(), {}, reviewer="rd", now=NOW)

    assert seed == "reconciliation_exception_reviews"
    assert row == {
        "entity": "bill", "canonical_id": "hr1-119", "disposition": "out_of_scope", "contract_version": "v1",
        "reviewer": "rd", "reviewed_at": "2026-10-12T17:30:00Z", "reason": "Not a bill.",
        "expires_at": "2027-10-12T00:00:00Z",
    }


def test_member_assignment_copies_context_and_uses_dates() -> None:
    request = DecisionRequest(key={"source_member_key": "k1"}, decision="assign", value="N000191",
                              reason="Unique name/state match.", expires_at="never")
    context = {"committee_id": "hsju00", "congress": "117", "state": "CO", "member_name": "Neguse Joe"}

    seed, row = build_seed_row(QUEUES["member-identities"], request, context, reviewer="rd", now=NOW)

    assert seed == "govinfo_member_identity_overrides"
    assert row["bioguide_id"] == "N000191"
    assert row["member_name"] == "Neguse Joe"
    assert row["reviewed_at"] == "2026-10-12"
    assert row["expires_at"] == ""


def test_committee_ids_route_each_decision_to_its_own_seed() -> None:
    queue = QUEUES["committee-ids"]
    mapped, _ = build_seed_row(queue, DecisionRequest({"source_committee_id": "hsa00"}, "map", "hsas00", "Armed Services.", None),
                               {}, reviewer="rd", now=NOW)
    excepted, row = build_seed_row(queue, DecisionRequest({"source_committee_id": "x"}, "reviewed_authority_exception", None, "Not a committee.", None),
                                   {}, reviewer="rd", now=NOW)

    assert mapped == "govinfo_committee_id_overrides"
    assert excepted == "govinfo_committee_id_exceptions"
    assert row["classification"] == "reviewed_authority_exception"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"decision": "maybe"}, "unknown decision"),
        ({"value": "X"}, "takes no --value"),
        ({"reason": "  "}, "reason is required"),
    ],
)
def test_invalid_requests_are_rejected(overrides: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        build_seed_row(QUEUES["match-exceptions"], _request(**overrides), {}, reviewer="rd", now=NOW)


def test_assign_requires_a_value() -> None:
    request = DecisionRequest({"source_member_key": "k1"}, "assign", None, "reason", None)
    with pytest.raises(ValueError, match="requires --value"):
        build_seed_row(QUEUES["member-identities"], request, {}, reviewer="rd", now=NOW)


def test_upsert_replaces_an_earlier_decision_for_the_same_key(tmp_path: Path) -> None:
    seed = tmp_path / "reviews.csv"
    seed.write_text(
        "entity,canonical_id,disposition,reason\n"
        "bill,hr1-119,data_error,old\n"
        "bill,hr2-119,out_of_scope,\"keep, quoted\"\n",
        encoding="utf-8",
    )

    action = upsert_seed_row(seed, ("entity", "canonical_id"),
                             {"entity": "bill", "canonical_id": "hr1-119", "disposition": "out_of_scope", "reason": "new"})

    assert action == "replaced"
    rows = list(csv.DictReader(seed.open(encoding="utf-8")))
    assert [(row["canonical_id"], row["reason"]) for row in rows] == [("hr2-119", "keep, quoted"), ("hr1-119", "new")]


def test_worksheet_skips_rows_without_a_decision(tmp_path: Path) -> None:
    worksheet = tmp_path / "sheet.csv"
    worksheet.write_text(
        "entity,canonical_id,missing_source,decision,value,reason,expires_at\n"
        "bill,hr1-119,govinfo,out_of_scope,,Not in scope.,\n"
        "bill,hr2-119,govinfo,,,,\n",
        encoding="utf-8",
    )

    [request] = read_worksheet(QUEUES["match-exceptions"], worksheet)

    assert request == DecisionRequest({"entity": "bill", "canonical_id": "hr1-119"}, "out_of_scope", None, "Not in scope.", None)


def test_decide_validates_every_row_before_writing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seed = tmp_path / "reconciliation_exception_reviews.csv"
    original = (reconcile.SEED_DIR / "reconciliation_exception_reviews.csv").read_text(encoding="utf-8")
    seed.write_text(original, encoding="utf-8")
    monkeypatch.setattr(reconcile, "SEED_DIR", tmp_path)
    worksheet = tmp_path / "sheet.csv"
    worksheet.write_text(
        "entity,canonical_id,decision,value,reason,expires_at\n"
        "bill,hr1-119,out_of_scope,,Fine.,\n"
        "bill,missing-1,out_of_scope,,Fine.,\n",
        encoding="utf-8",
    )
    existing = {"hr1-119"}
    monkeypatch.setattr(reconcile, "fetch_queue_row",
                        lambda connection, queue, key: {"found": 1} if key["canonical_id"] in existing else None)
    args = reconcile.parse_args(["decide", "match-exceptions", "--worksheet", str(worksheet), "--reviewer", "rd"])

    with pytest.raises(ValueError, match="no row"):
        reconcile.command_decide(None, QUEUES["match-exceptions"], args)  # type: ignore[arg-type]

    assert seed.read_text(encoding="utf-8") == original
