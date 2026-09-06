from __future__ import annotations

from pathlib import Path
import subprocess


WRAPPER = Path(__file__).resolve().parents[2] / "scripts" / "run_congress_supplemental_refresh.sh"


def test_supplemental_refresh_wrapper_has_valid_shell_and_combined_gate() -> None:
    syntax = subprocess.run(
        ["bash", "-n", str(WRAPPER)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert syntax.returncode == 0, syntax.stderr

    source = WRAPPER.read_text(encoding="utf-8")
    assert 'echo "quality_gate_status=$quality_gate_status"' in source
    assert 'echo "combined_release_gate=PASS"' in source
    assert 'echo "combined_release_gate=FAIL"' in source
    assert "dbt_run_status == 0" in source
    assert "dbt_test_status == 0" in source
