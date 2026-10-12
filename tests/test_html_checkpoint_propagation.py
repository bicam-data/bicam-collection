from __future__ import annotations

import fcntl
import json
import os
import re
import stat
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts import run_html_text_lane as lane  # noqa: E402

RUN_LOG_LINE = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ (\{.*\})$")
LANE_VARIABLES = (
    "BICAM_HTML_REPORT_DIR", "BICAM_HTML_BATCH_LIMIT", "BICAM_HTML_MAX_BATCHES", "BICAM_HTML_CONCURRENCY",
    "BICAM_HTML_REQUEST_INTERVAL", "BICAM_HTML_CONGRESS_REQUEST_INTERVAL", "BICAM_HTML_GOVINFO_REQUEST_INTERVAL",
    "BICAM_HTML_REFRESH_DAYS", "BICAM_HTML_RETRY_FAILED_DAYS", "BICAM_HTML_REQUEST_TIMEOUT", "BICAM_HTML_MAX_BYTES",
    "BICAM_HTML_DBT_BATCH_INTERVAL", "BICAM_HTML_SKIP_DBT", "BICAM_HTML_DBT_FORCE",
    "BICAM_HTML_DBT_TIMEOUT_SECONDS", "BICAM_HTML_DBT_STATEMENT_TIMEOUT_MS",
    "BICAM_HTML_DBT_TEST_TIMEOUT_SECONDS", "BICAM_HTML_DBT_TEST_STATEMENT_TIMEOUT_MS",
    "BICAM_HTML_LOCK_TIMEOUT_SECONDS", "BICAM_INGESTION_LOCK_HELD", "PGOPTIONS",
)

# The fake fetch script answers from a per-table queue of canned results and
# records its argument list, environment, and whether the ingestion lock was
# free while it ran.  An empty queue behaves like the real script with no
# eligible URLs: nothing requested and the cursor echoed back.
FAKE_FETCH = """
import fcntl, json, os, sys
from pathlib import Path
calls_dir = Path(os.environ["FAKE_CALLS_DIR"])
queue_path = calls_dir / "fetch_queue.json"
queue = json.loads(queue_path.read_text())
table = sys.argv[sys.argv.index("--source-table") + 1]
after = sys.argv[sys.argv.index("--after") + 1] if "--after" in sys.argv else None
with open(os.environ["FAKE_LOCK_PATH"], "a") as lock:
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_free = True
    except BlockingIOError:
        lock_free = False
with (calls_dir / "fetch_calls.jsonl").open("a") as calls:
    calls.write(json.dumps({"argv": sys.argv[1:], "lock_free": lock_free, "dotenv_value": os.environ.get("LANE_DOTENV_VALUE")}) + "\\n")
if queue.get(table):
    result = queue[table].pop(0)
    queue_path.write_text(json.dumps(queue))
else:
    result = {"requested": 0, "last_url": after, "statuses": {}}
print(json.dumps(result, sort_keys=True))
"""

FAKE_REPORT = """
import os, sys
from pathlib import Path
Path(os.environ["FAKE_CALLS_DIR"], "report_calls.txt").open("a").write(" ".join(sys.argv[1:]) + "\\n")
"""

# The fake ``uv`` stands in for ``uv run dbt ...`` and for the shim's
# ``uv run python ...``.  It records its arguments, working directory,
# PGAPPNAME/PGOPTIONS, and the lock state, then exits with the status staged
# for that dbt subcommand.  ``dbt_hang`` makes it sleep past the client
# timeout, optionally ignoring TERM so the KILL path is exercised.
FAKE_UV = """
import fcntl, json, os, signal, sys, time
from pathlib import Path
calls_dir = Path(os.environ["FAKE_CALLS_DIR"])
with open(os.environ["FAKE_LOCK_PATH"], "a") as lock:
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_free = True
    except BlockingIOError:
        lock_free = False
with (calls_dir / "uv_calls.jsonl").open("a") as calls:
    calls.write(json.dumps({
        "argv": sys.argv[1:], "cwd": os.getcwd(), "lock_free": lock_free,
        "PGAPPNAME": os.environ.get("PGAPPNAME"), "PGOPTIONS": os.environ.get("PGOPTIONS"),
    }) + "\\n")
args = sys.argv[1:]
if args[:2] == ["run", "python"]:
    # The copied legacy wrapper invokes inline stdlib helpers and the two
    # fixture scripts. Never dispatch arbitrary application/production code.
    if args[2] not in {"-", "scripts/fetch_html_text.py", "scripts/report_html_text_progress.py"}:
        raise SystemExit("unapproved Python command in scheduler test")
    os.execv(sys.executable, [sys.executable, *args[2:]])
subcommand = args[2] if args[:2] == ["run", "dbt"] else None
hang = calls_dir / "dbt_hang"
if subcommand == "run" and hang.exists():
    if hang.read_text() == "ignore-term":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(30)
status_file = calls_dir / f"dbt_{subcommand}_status"
sys.exit(int(status_file.read_text()) if status_file.exists() else 0)
"""

FAKE_DOCKER = """
import json, os, sys
from pathlib import Path
Path(os.environ["FAKE_CALLS_DIR"], "docker_calls.jsonl").open("a").write(json.dumps(sys.argv[1:]) + "\\n")
"""


def _install_script(path: Path, body: str) -> None:
    path.write_text(f"#!{sys.executable}\n{textwrap.dedent(body)}", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


@dataclass
class Harness:
    repo: Path
    report_dir: Path
    calls_dir: Path
    lock_path: Path

    @property
    def checkpoint_path(self) -> Path:
        return self.report_dir / "checkpoint.json"

    def write_checkpoint(self, payload: dict) -> None:
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def read_checkpoint(self) -> dict:
        return json.loads(self.checkpoint_path.read_text(encoding="utf-8"))

    def queue_results(self, queue: dict[str, list[dict]]) -> None:
        (self.calls_dir / "fetch_queue.json").write_text(json.dumps(queue))

    def _calls(self, name: str) -> list[dict]:
        path = self.calls_dir / name
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def fetch_calls(self) -> list[dict]:
        return self._calls("fetch_calls.jsonl")

    def uv_calls(self) -> list[dict]:
        return self._calls("uv_calls.jsonl")

    def dbt_calls(self) -> list[dict]:
        return [call for call in self.uv_calls() if call["argv"][:2] == ["run", "dbt"]]

    def docker_calls(self) -> list[list[str]]:
        return self._calls("docker_calls.jsonl")

    def run_log(self) -> list[str]:
        return (self.report_dir / "run.log").read_text(encoding="utf-8").splitlines()


@pytest.fixture
def harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Harness:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    calls_dir = tmp_path / "calls"
    calls_dir.mkdir()
    shims = tmp_path / "bin"
    shims.mkdir()
    lock_path = tmp_path / "bicam-ingestion.lock"
    _install_script(repo / "scripts" / "fetch_html_text.py", FAKE_FETCH)
    _install_script(repo / "scripts" / "report_html_text_progress.py", FAKE_REPORT)
    _install_script(shims / "uv", FAKE_UV)
    _install_script(shims / "docker", FAKE_DOCKER)
    harness = Harness(repo=repo, report_dir=tmp_path / "reports", calls_dir=calls_dir, lock_path=lock_path)
    harness.queue_results({})

    for name in LANE_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PATH", f"{shims}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_CALLS_DIR", str(calls_dir))
    monkeypatch.setenv("FAKE_LOCK_PATH", str(lock_path))
    monkeypatch.setenv("BICAM_HTML_REPORT_DIR", str(harness.report_dir))
    monkeypatch.setattr(lane, "REPO_ROOT", repo)
    monkeypatch.setattr(lane, "LOCK_PATH", lock_path)
    # Backend cancellation pauses two seconds between passes; the fakes do not
    # need the real pause.
    monkeypatch.setattr(lane.time, "sleep", lambda seconds: None)
    return harness


def full_batch(last_url: str, limit: int = 2) -> dict:
    return {"requested": limit, "last_url": last_url, "statuses": {"success": limit}}


LIVE_SHAPED_CHECKPOINT = {
    "dbt_batches_since_last": 0,
    "last_urls": {
        "amendments_texts": "",
        "bills_texts": "https://www.congress.gov/104/bills/s369/BILLS-104s369rfh.htm",
        "bills_versions": "https://api.govinfo.gov/packages/BILLS-106sres287ats/htm",
        "committee_prints_texts": "",
        "committee_reports_texts": "",
        "hearings_texts": "",
    },
    "next_source_table": "amendments_texts",
    "pending_dbt": 0,
    "requested": 250,
    "statuses": {"success": 250},
}


def test_resumes_live_checkpoint_rotates_and_logs(harness: Harness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    harness.write_checkpoint(LIVE_SHAPED_CHECKPOINT)
    harness.queue_results({
        "amendments_texts": [full_batch("https://www.congress.gov/amendment/a2.htm")],
        "bills_texts": [full_batch("https://www.congress.gov/104/bills/s400/BILLS-104s400ih.htm")],
    })

    assert lane.main() == 0

    [call] = harness.fetch_calls()
    assert call["argv"] == [
        "--source-table", "amendments_texts",
        "--limit", "2", "--concurrency", "12", "--request-interval", "0.08",
        "--congress-request-interval", "1.0", "--govinfo-request-interval", "0.25",
        "--refresh-days", "30", "--retry-failed-days", "1", "--timeout", "12", "--max-bytes", "20000000",
    ]
    checkpoint = harness.read_checkpoint()
    assert checkpoint == {
        "dbt_batches_since_last": 1,
        "last_urls": {**LIVE_SHAPED_CHECKPOINT["last_urls"], "amendments_texts": "https://www.congress.gov/amendment/a2.htm"},
        "next_source_table": "bills_texts",
        "pending_dbt": 1,
        "requested": 2,
        "statuses": {"success": 2},
    }
    assert harness.checkpoint_path.read_text() == json.dumps(checkpoint, indent=2, sort_keys=True) + "\n"
    [logged] = harness.run_log()
    match = RUN_LOG_LINE.match(logged)
    assert match is not None
    assert json.loads(match.group(1)) == full_batch("https://www.congress.gov/amendment/a2.htm")
    out = capsys.readouterr().out.splitlines()
    assert "html_dbt_deferred=1 batches_since_last=1 interval=6" in out
    assert out[-1] == f"html_text_checkpoint={harness.checkpoint_path}"
    assert (harness.calls_dir / "report_calls.txt").read_text() == f"--output-dir {harness.report_dir}\n"
    assert harness.dbt_calls() == []

    # The next tick resumes on bills_texts from its own persisted cursor.
    assert lane.main() == 0
    assert harness.fetch_calls()[1]["argv"][:2] == ["--source-table", "bills_texts"]
    assert harness.fetch_calls()[1]["argv"][-2:] == ["--after", "https://www.congress.gov/104/bills/s369/BILLS-104s369rfh.htm"]
    assert harness.read_checkpoint()["next_source_table"] == "committee_prints_texts"


def test_exhausted_table_resets_cursor_without_consuming_the_batch_budget(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    harness.queue_results({
        "amendments_texts": [{"requested": 1, "last_url": "https://a/1", "statuses": {"success": 1}}],
        "bills_texts": [full_batch("https://b/2")],
    })

    assert lane.main() == 0

    tables = [call["argv"][1] for call in harness.fetch_calls()]
    assert tables == ["amendments_texts", "bills_texts"]
    checkpoint = harness.read_checkpoint()
    assert checkpoint["last_urls"]["amendments_texts"] == ""
    assert checkpoint["last_urls"]["bills_texts"] == "https://b/2"
    assert checkpoint["next_source_table"] == "committee_prints_texts"
    assert checkpoint["dbt_batches_since_last"] == 2


def test_all_exhausted_queue_is_bounded(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")

    assert lane.main() == 0

    assert len(harness.fetch_calls()) == 1 + len(lane.SOURCE_TABLES)
    checkpoint = harness.read_checkpoint()
    assert checkpoint["next_source_table"] == "bills_texts"
    assert checkpoint["pending_dbt"] == 0
    assert set(checkpoint["last_urls"].values()) == {""}


def test_propagation_runs_at_the_batch_interval(harness: Harness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    monkeypatch.setenv("BICAM_HTML_DBT_BATCH_INTERVAL", "2")
    harness.write_checkpoint({**LIVE_SHAPED_CHECKPOINT, "pending_dbt": 1, "dbt_batches_since_last": 1})
    harness.queue_results({"amendments_texts": [full_batch("https://a/2")]})

    assert lane.main() == 0

    run_call, test_call = harness.dbt_calls()
    assert run_call["argv"] == [
        "run", "dbt", "run", "--no-partial-parse", "--project-dir", "dbt_project", "--profiles-dir", "dbt_project",
        "--select", *lane.DBT_SELECT,
    ]
    assert test_call["argv"] == [
        "run", "dbt", "test", "--no-partial-parse", "--project-dir", "dbt_project", "--profiles-dir", "dbt_project",
        "--select", "html_successful_extractions_have_text", "html_successful_current_prefers_last_success",
    ]
    assert re.fullmatch(r"bicam-html-dbt-\d{8}T\d{6}Z-\d+", run_call["PGAPPNAME"])
    assert test_call["PGAPPNAME"] == run_call["PGAPPNAME"] + "-test"
    assert run_call["PGOPTIONS"] == " -c statement_timeout=1740000"
    assert test_call["PGOPTIONS"] == " -c statement_timeout=270000"
    checkpoint = harness.read_checkpoint()
    assert checkpoint["pending_dbt"] == 0
    assert checkpoint["dbt_batches_since_last"] == 0
    out = capsys.readouterr().out.splitlines()
    assert "html_dbt_run_status=0" in out
    assert "html_dbt_test_status=0" in out
    # The exit-time sweep cancels only this run's application name.
    assert [call[-1].split("from")[0].strip() for call in harness.docker_calls()] == [
        "select pg_cancel_backend(pid)", "select pg_cancel_backend(pid)", "select pg_terminate_backend(pid)",
    ]
    assert {call[call.index("-v") + 1] for call in harness.docker_calls()} == {f"app_name={run_call['PGAPPNAME']}"}


def test_propagation_runs_at_the_cycle_boundary(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    harness.write_checkpoint({**LIVE_SHAPED_CHECKPOINT, "next_source_table": "bills_versions"})
    harness.queue_results({"bills_versions": [full_batch("https://api.govinfo.gov/packages/BILLS-107/htm")]})

    assert lane.main() == 0

    assert harness.read_checkpoint()["next_source_table"] == "amendments_texts"
    assert [call["argv"][2] for call in harness.dbt_calls()] == ["run", "test"]


def test_propagation_is_deferred_before_the_interval(harness: Harness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    harness.write_checkpoint({**LIVE_SHAPED_CHECKPOINT, "next_source_table": "bills_texts", "pending_dbt": 1, "dbt_batches_since_last": 3})
    harness.queue_results({"bills_texts": [full_batch("https://b/2")]})

    assert lane.main() == 0

    assert harness.dbt_calls() == []
    assert "html_dbt_deferred=1 batches_since_last=4 interval=6" in capsys.readouterr().out.splitlines()
    assert harness.read_checkpoint()["dbt_batches_since_last"] == 4


def test_skip_dbt_leaves_pending_state_untouched(harness: Harness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    monkeypatch.setenv("BICAM_HTML_SKIP_DBT", "1")
    monkeypatch.setenv("BICAM_HTML_DBT_FORCE", "1")
    harness.queue_results({"amendments_texts": [full_batch("https://a/2")]})

    assert lane.main() == 0

    assert harness.dbt_calls() == []
    assert "html_dbt_deferred=1 batches_since_last=1 interval=6" in capsys.readouterr().out.splitlines()
    assert harness.read_checkpoint()["pending_dbt"] == 1


def test_dbt_failure_keeps_pending_and_zeroes_the_counter(harness: Harness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    monkeypatch.setenv("BICAM_HTML_DBT_FORCE", "1")
    (harness.calls_dir / "dbt_run_status").write_text("1")
    harness.queue_results({"amendments_texts": [full_batch("https://a/2")]})

    assert lane.main() == 0

    assert [call["argv"][2] for call in harness.dbt_calls()] == ["run"]
    out = capsys.readouterr().out.splitlines()
    assert "html_dbt_test_skipped=1 reason=dbt_run_status_1" in out
    assert "html_dbt_run_status=1" in out
    assert "html_dbt_test_status=0" in out
    assert "html_dbt_pending=1 reason=propagation_failed" in out
    checkpoint = harness.read_checkpoint()
    assert checkpoint["pending_dbt"] == 1
    assert checkpoint["dbt_batches_since_last"] == 0
    assert checkpoint["last_urls"]["amendments_texts"] == "https://a/2"
    # Explicit cancellation on failure plus the exit-time sweep.
    assert len(harness.docker_calls()) == 6


def test_dbt_test_failure_cancels_the_test_backends(harness: Harness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    monkeypatch.setenv("BICAM_HTML_DBT_FORCE", "1")
    (harness.calls_dir / "dbt_test_status").write_text("1")
    harness.queue_results({"amendments_texts": [full_batch("https://a/2")]})

    assert lane.main() == 0

    run_call, test_call = harness.dbt_calls()
    out = capsys.readouterr().out.splitlines()
    assert "html_dbt_run_status=0" in out
    assert "html_dbt_test_status=1" in out
    assert "html_dbt_pending=1 reason=propagation_failed" in out
    app_names = [call[call.index("-v") + 1] for call in harness.docker_calls()]
    assert app_names[:3] == [f"app_name={test_call['PGAPPNAME']}"] * 3
    assert app_names[3:] == [f"app_name={run_call['PGAPPNAME']}"] * 3
    assert harness.read_checkpoint()["pending_dbt"] == 1


@pytest.mark.parametrize(("hang_mode", "expected_status"), [("terminates", 124), ("ignore-term", 137)])
def test_dbt_timeout_terminates_then_kills(harness: Harness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], hang_mode: str, expected_status: int) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    monkeypatch.setenv("BICAM_HTML_DBT_FORCE", "1")
    monkeypatch.setenv("BICAM_HTML_DBT_TIMEOUT_SECONDS", "0.5")
    monkeypatch.setattr(lane, "DBT_KILL_AFTER_SECONDS", 0.5)
    (harness.calls_dir / "dbt_hang").write_text(hang_mode)
    harness.queue_results({"amendments_texts": [full_batch("https://a/2")]})

    assert lane.main() == 0

    out = capsys.readouterr().out.splitlines()
    assert f"html_dbt_run_status={expected_status}" in out
    assert "html_dbt_pending=1 reason=propagation_failed" in out


def test_lock_is_held_only_during_propagation(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    monkeypatch.setenv("BICAM_HTML_DBT_FORCE", "1")
    harness.queue_results({"amendments_texts": [full_batch("https://a/2")]})

    assert lane.main() == 0

    assert [call["lock_free"] for call in harness.fetch_calls()] == [True]
    assert [call["lock_free"] for call in harness.dbt_calls()] == [False, False]
    # The lock is released again before the run ends.
    with harness.lock_path.open("a") as probe:
        fcntl.flock(probe.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_lock_busy_defers_propagation_but_still_fetches(harness: Harness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    monkeypatch.setenv("BICAM_HTML_DBT_FORCE", "1")
    monkeypatch.setenv("BICAM_HTML_LOCK_TIMEOUT_SECONDS", "0")
    harness.queue_results({"amendments_texts": [full_batch("https://a/2")]})

    with harness.lock_path.open("a") as other_lane:
        fcntl.flock(other_lane.fileno(), fcntl.LOCK_EX)
        assert lane.main() == 0

    assert len(harness.fetch_calls()) == 1
    assert harness.dbt_calls() == []
    fetch_line, busy_line = harness.run_log()
    assert RUN_LOG_LINE.match(fetch_line)
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ \{\"event\":\"lock_busy\",\"retry\":\"next_timer\"\}", busy_line)
    out = capsys.readouterr().out.splitlines()
    assert "html_text_lock_busy; retrying on the next timer interval" in out
    assert out[-1] == f"html_text_checkpoint={harness.checkpoint_path}"
    checkpoint = harness.read_checkpoint()
    assert checkpoint["pending_dbt"] == 1
    assert checkpoint["dbt_batches_since_last"] == 1
    assert harness.docker_calls() == []


def test_lock_held_marker_skips_acquisition(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    monkeypatch.setenv("BICAM_HTML_DBT_FORCE", "1")
    monkeypatch.setenv("BICAM_HTML_LOCK_TIMEOUT_SECONDS", "0")
    monkeypatch.setenv("BICAM_INGESTION_LOCK_HELD", "1")
    harness.queue_results({"amendments_texts": [full_batch("https://a/2")]})

    with harness.lock_path.open("a") as outer_wrapper:
        fcntl.flock(outer_wrapper.fileno(), fcntl.LOCK_EX)
        assert lane.main() == 0

    assert [call["argv"][2] for call in harness.dbt_calls()] == ["run", "test"]
    assert harness.read_checkpoint()["pending_dbt"] == 0


def test_dotenv_is_exported_to_subprocesses(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BICAM_HTML_BATCH_LIMIT", "2")
    monkeypatch.setenv("LANE_DOTENV_VALUE", "from-environment")
    (harness.repo / ".env").write_text("\n# comment\nLANE_DOTENV_VALUE=from-dotenv\n", encoding="utf-8")
    harness.queue_results({"amendments_texts": [full_batch("https://a/2")]})

    assert lane.main() == 0

    assert harness.fetch_calls()[0]["dotenv_value"] == "from-dotenv"


def test_invalid_dbt_batch_interval_exits_two(harness: Harness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("BICAM_HTML_DBT_BATCH_INTERVAL", "0")

    assert lane.main() == 2

    assert capsys.readouterr().err == "invalid BICAM_HTML_DBT_BATCH_INTERVAL=0 (expected positive integer)\n"
    assert harness.fetch_calls() == []


def test_fetch_failure_propagates_its_exit_status(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    (harness.repo / "scripts" / "fetch_html_text.py").write_text(f"#!{sys.executable}\nraise SystemExit(3)\n")

    with pytest.raises(SystemExit) as exit_info:
        lane.main()

    assert exit_info.value.code == 3


def test_dbt_selection_rebuilds_resolver_ancestors() -> None:
    assert "int_congress_child_response_selection" in lane.DBT_SELECT
    assert "int_congress_child_response_selections" in lane.DBT_SELECT
    assert "stg_congress__committee_report_detail" in lane.DBT_SELECT
    assert "int_committee_print_public_id_map" in lane.DBT_SELECT
    for leaf in lane.SOURCE_TABLES:
        assert leaf in lane.DBT_SELECT
    assert not any(model.startswith("+") for model in lane.DBT_SELECT)


def test_dbt_profile_accepts_run_specific_application_names() -> None:
    profiles = (REPO_ROOT / "dbt_project" / "profiles.yml").read_text(encoding="utf-8")

    assert 'application_name: "{{ env_var(\'PGAPPNAME\', \'dbt\') }}"' in profiles


def test_existing_shell_wrapper_isolated_from_live_checkout(harness: Harness, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Production still uses the legacy shell wrapper. Exercise that actual
    # file in a copied repository with fake fetch/dbt/docker commands, not a
    # hypothetical Python shim and never the operational repo/shared lock.
    wrapper = harness.repo / 'scripts/run_html_text_batches.sh'
    wrapper.write_bytes((REPO_ROOT / 'scripts/run_html_text_batches.sh').read_bytes())
    monkeypatch.setenv('BICAM_INGESTION_LOCK_HELD', '1')
    monkeypatch.setenv('BICAM_HTML_SKIP_DBT', '1')
    monkeypatch.setenv('BICAM_HTML_BATCH_LIMIT', '2')
    monkeypatch.setenv('BICAM_HTML_MAX_BATCHES', '1')
    harness.queue_results({'amendments_texts': [full_batch('https://example.test/a2.htm')]})
    completed = subprocess.run(
        ["bash", str(wrapper)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr
    [fetch] = harness.fetch_calls()
    assert fetch['argv'][:2] == ['--source-table', 'amendments_texts']
    assert harness.read_checkpoint()['last_urls']['amendments_texts'] == 'https://example.test/a2.htm'
    assert harness.dbt_calls() == []
    assert all(Path(call['cwd']).resolve() == harness.repo for call in harness.uv_calls())
