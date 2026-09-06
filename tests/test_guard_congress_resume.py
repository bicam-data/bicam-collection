from pathlib import Path


GUARD_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "guard_congress_resume.sh"


def test_guard_counts_transport_failures_not_deterministic_500s() -> None:
    text = GUARD_SCRIPT.read_text(encoding="utf-8")

    assert "count(*) filter (where outcome = 'failed' and http_status is distinct from 500)" in text
    assert "count(*) filter (where outcome = 'failed' and http_status = 503)" in text
    assert "count(*) filter (where outcome = 'failed' and http_status = 429)" in text
    assert "count(*) filter (where outcome = 'fetched')" in text
    assert "quota_cooldown" in text
    assert "failures_429 == total" in text
    assert "fetched == 0" in text
    assert "resume_stopped_workers" in text
    assert "ps -eo pid=,stat=,args=" in text
    assert "kill -CONT \"${stopped_pids[@]}\"" in text
