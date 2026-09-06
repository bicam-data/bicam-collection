from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

from bicam_collection.target_scope import TARGET_CONGRESS_CATEGORIES

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def _load_scheduler_module():
    path = PROJECT_ROOT / "scripts" / "run_scheduled_batch.py"
    spec = importlib.util.spec_from_file_location("run_scheduled_batch", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SCHEDULER = _load_scheduler_module()


class SchedulerContractTests(unittest.TestCase):
    def test_schedule_has_bounded_job_and_expected_commands(self) -> None:
        schedule = SCHEDULER.load_schedule(PROJECT_ROOT)
        job = schedule["jobs"]["bounded_congress"]
        self.assertEqual(job["categories"], list(TARGET_CONGRESS_CATEGORIES))
        commands = SCHEDULER.build_commands(project_root=PROJECT_ROOT, job=job, verbose=False)
        self.assertEqual(len(commands), 3)
        self.assertIn("--max-pages", commands[0])
        self.assertIn("--overlap-hours", commands[0])
        self.assertIn("incremental", commands[0])
        self.assertIn("members", commands[0])
        self.assertIn("--no-partial-parse", commands[1])
        self.assertIn("--max-open-exceptions", commands[2])

    def test_dry_run_does_not_require_database(self) -> None:
        self.assertEqual(SCHEDULER.run(["--job", "bounded_congress", "--dry-run"], project_root=PROJECT_ROOT), 0)


if __name__ == "__main__":
    unittest.main()
