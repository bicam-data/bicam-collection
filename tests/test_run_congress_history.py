from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_congress_history.py"
_SPEC = importlib.util.spec_from_file_location("run_congress_history", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)
Job = _MODULE.Job
_ordered_jobs = _MODULE._ordered_jobs
parse_args = _MODULE.parse_args
_classify_partition_exception = _MODULE._classify_partition_exception


def test_historical_jobs_interleave_partitioned_categories() -> None:
    assert _ordered_jobs(["bills", "amendments"], 1, 2) == [
        Job("bills", 1),
        Job("amendments", 1),
        Job("bills", 2),
        Job("amendments", 2),
    ]


def test_historical_jobs_append_unpartitioned_categories() -> None:
    assert _ordered_jobs(["bills", "members", "amendments", "congresses"], 1, 1) == [
        Job("bills", 1),
        Job("amendments", 1),
        Job("members", None),
        Job("congresses", None),
    ]


def test_new_endpoint_families_use_their_documented_partition_shapes() -> None:
    assert _ordered_jobs(
        ["laws", "house-communications", "house-requirements", "crs-reports"],
        118,
        119,
    ) == [
        Job("laws", 118),
        Job("house-communications", 118),
        Job("laws", 119),
        Job("house-communications", 119),
        Job("house-requirements", None),
        Job("crs-reports", None),
    ]


def test_partition_exception_classifies_transient_http_status() -> None:
    class Error(Exception):
        response = type("Response", (), {"status_code": 503})()

    assert _classify_partition_exception(Error("service unavailable")) == "retryable"


def test_partition_exception_classifies_source_500_without_aborting_worker() -> None:
    class Error(Exception):
        response = type("Response", (), {"status_code": 500})()

    assert _classify_partition_exception(Error("source_error=member_term_does_not_exist")) == "source_error"


def test_partition_exception_classifies_configuration_errors_as_fatal() -> None:
    assert _classify_partition_exception(ValueError("invalid spec")) == "fatal"
