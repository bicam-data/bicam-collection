from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch

from bicam_collection.config import Settings
from bicam_collection.spec_loader import discover_spec_categories, resolve_spec_path
from bicam_collection.target_scope import TARGET_CONGRESS_CATEGORIES


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN_CRAWL_PATH = PROJECT_ROOT / "scripts" / "run_crawl.py"
RUN_SAMPLE_CRAWL_PATH = PROJECT_ROOT / "scripts" / "run_sample_crawl.py"
RUN_MULTI_CRAWL_PATH = PROJECT_ROOT / "scripts" / "run_multi_crawl.py"


def _load_run_crawl_module():
    spec = importlib.util.spec_from_file_location("run_crawl", RUN_CRAWL_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_run_sample_crawl_module():
    spec = importlib.util.spec_from_file_location("run_sample_crawl", RUN_SAMPLE_CRAWL_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_run_multi_crawl_module():
    spec = importlib.util.spec_from_file_location("run_multi_crawl", RUN_MULTI_CRAWL_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CliTests(unittest.TestCase):
    def test_discover_spec_categories_lists_available_yaml_specs(self) -> None:
        categories = discover_spec_categories(PROJECT_ROOT / "specs")

        self.assertEqual(
            categories,
            [
                "amendments",
                "bills",
                "bound-congressional-record",
                "committee-meetings",
                "committee-prints",
                "committee-reports",
                "committees",
                "congresses",
                "congressional-record",
                "crs-reports",
                "daily-congressional-record",
                "govinfo-bills",
                "govinfo-committee-prints",
                "govinfo-committee-reports",
                "govinfo-congressional-directories",
                "govinfo-hearings",
                "govinfo-treaty-documents",
                "hearings",
                "house-communications",
                "house-requirements",
                "house-votes",
                "laws",
                "members",
                "nominations",
                "senate-communications",
                "summaries",
                "treaties",
            ],
        )

    def test_parse_args_accepts_bills_committee_meetings_committee_prints_committee_reports_committees_congresses_govinfo_hearings_members_nominations_and_treaties(self) -> None:
        run_crawl = _load_run_crawl_module()

        bills_args = run_crawl.parse_args(["--category", "bills", "--mode", "full"], project_root=PROJECT_ROOT)
        committee_meetings_args = run_crawl.parse_args(["--category", "committee-meetings", "--mode", "full"], project_root=PROJECT_ROOT)
        committee_prints_args = run_crawl.parse_args(["--category", "committee-prints", "--mode", "full"], project_root=PROJECT_ROOT)
        committee_reports_args = run_crawl.parse_args(["--category", "committee-reports", "--mode", "full"], project_root=PROJECT_ROOT)
        committees_args = run_crawl.parse_args(["--category", "committees", "--mode", "full"], project_root=PROJECT_ROOT)
        congresses_args = run_crawl.parse_args(["--category", "congresses", "--mode", "full"], project_root=PROJECT_ROOT)
        govinfo_committee_reports_args = run_crawl.parse_args(["--category", "govinfo-committee-reports", "--mode", "full"], project_root=PROJECT_ROOT)
        hearings_args = run_crawl.parse_args(["--category", "hearings", "--mode", "full"], project_root=PROJECT_ROOT)
        members_args = run_crawl.parse_args(["--category", "members", "--mode", "full"], project_root=PROJECT_ROOT)
        nominations_args = run_crawl.parse_args(["--category", "nominations", "--mode", "full"], project_root=PROJECT_ROOT)
        treaties_args = run_crawl.parse_args(["--category", "treaties", "--mode", "full"], project_root=PROJECT_ROOT)

        self.assertEqual(bills_args.category, "bills")
        self.assertEqual(committee_meetings_args.category, "committee-meetings")
        self.assertEqual(committee_prints_args.category, "committee-prints")
        self.assertEqual(committee_reports_args.category, "committee-reports")
        self.assertEqual(committees_args.category, "committees")
        self.assertEqual(congresses_args.category, "congresses")
        self.assertEqual(govinfo_committee_reports_args.category, "govinfo-committee-reports")
        self.assertEqual(hearings_args.category, "hearings")
        self.assertEqual(members_args.category, "members")
        self.assertEqual(nominations_args.category, "nominations")
        self.assertEqual(treaties_args.category, "treaties")

    def test_parse_args_supports_async_flags(self) -> None:
        run_crawl = _load_run_crawl_module()

        args = run_crawl.parse_args(["--category", "bills", "--mode", "full", "--async", "--concurrency", "7"], project_root=PROJECT_ROOT)

        self.assertTrue(args.use_async)
        self.assertEqual(args.concurrency, 7)

    def test_resolve_spec_path_raises_clear_error_for_missing_category(self) -> None:
        with self.assertRaisesRegex(FileNotFoundError, "Spec not found for category 'communications'"):
            resolve_spec_path(PROJECT_ROOT / "specs", "communications")

    def test_sample_parse_args_defaults_to_target_categories_and_bounded_pages(self) -> None:
        run_sample_crawl = _load_run_sample_crawl_module()

        args = run_sample_crawl.parse_args([], project_root=PROJECT_ROOT)

        self.assertEqual(args.categories, list(TARGET_CONGRESS_CATEGORIES))
        self.assertEqual(args.max_pages, 3)
        self.assertEqual(args.page_size, 25)
        self.assertFalse(args.use_async)
        self.assertEqual(args.concurrency, 5)
        self.assertEqual(args.category_workers, 3)
        self.assertEqual(args.dbt, "skip")

    def test_sample_main_runs_selected_categories_and_optional_dbt(self) -> None:
        run_sample_crawl = _load_run_sample_crawl_module()
        categories_run: list[list[str]] = []
        dbt_actions: list[str] = []

        with patch.object(run_sample_crawl, "run_single_crawl_main", side_effect=lambda argv: categories_run.append(argv) or 0), patch.object(
            run_sample_crawl, "_run_dbt", side_effect=lambda project_root, action: dbt_actions.append(action)
        ), patch.object(
            Settings,
            "from_env",
            classmethod(
                lambda cls, project_root: type(
                    "SettingsStub",
                    (),
                    {"database_url": "postgres://example", "env_path": project_root / ".env"},
                )()
            ),
        ):
            exit_code = run_sample_crawl.main(["--categories", "bills", "members", "--max-pages", "5", "--dbt", "run"])

        self.assertEqual(exit_code, 0)
        self.assertCountEqual(
            categories_run,
            [
                ["--category", "bills", "--mode", "full", "--max-pages", "5", "--page-size", "25"],
                ["--category", "members", "--mode", "full", "--max-pages", "5", "--page-size", "25"],
            ],
        )
        self.assertEqual(dbt_actions, ["run"])

    def test_sample_main_passes_async_flags_through(self) -> None:
        run_sample_crawl = _load_run_sample_crawl_module()
        categories_run: list[list[str]] = []

        with patch.object(run_sample_crawl, "run_single_crawl_main", side_effect=lambda argv: categories_run.append(argv) or 0), patch.object(
            run_sample_crawl, "_run_dbt", return_value=None
        ), patch.object(
            Settings,
            "from_env",
            classmethod(
                lambda cls, project_root: type(
                    "SettingsStub",
                    (),
                    {"database_url": "postgres://example", "env_path": project_root / ".env"},
                )()
            ),
        ):
            exit_code = run_sample_crawl.main(["--categories", "bills", "--async", "--concurrency", "9"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            categories_run,
            [["--category", "bills", "--mode", "full", "--max-pages", "3", "--page-size", "25", "--async", "--concurrency", "9"]],
        )

    def test_sample_parse_args_supports_category_workers(self) -> None:
        run_sample_crawl = _load_run_sample_crawl_module()

        args = run_sample_crawl.parse_args(["--category-workers", "4"], project_root=PROJECT_ROOT)

        self.assertEqual(args.category_workers, 4)

    def test_multi_parse_args_defaults_to_target_categories_without_sample_limits(self) -> None:
        run_multi_crawl = _load_run_multi_crawl_module()

        args = run_multi_crawl.parse_args([], project_root=PROJECT_ROOT)

        self.assertEqual(args.categories, list(TARGET_CONGRESS_CATEGORIES))
        self.assertIsNone(args.max_pages)
        self.assertIsNone(args.page_size)
        self.assertEqual(args.concurrency, 5)
        self.assertEqual(args.category_workers, 2)
        self.assertEqual(args.overlap_hours, 0.0)
        self.assertEqual(args.cleanup_stale_hours, 1.0)

    def test_multi_parse_args_supports_incremental_overlap(self) -> None:
        run_multi_crawl = _load_run_multi_crawl_module()

        args = run_multi_crawl.parse_args(
            ["--mode", "incremental", "--overlap-hours", "24"],
            project_root=PROJECT_ROOT,
        )

        self.assertEqual(args.overlap_hours, 24.0)

    def test_multi_main_dispatches_to_async_runner(self) -> None:
        run_multi_crawl = _load_run_multi_crawl_module()
        captured_args = {}

        def fake_asyncio_run(coro):
            try:
                result = coro.send(None)
            except StopIteration as exc:
                return exc.value
            raise AssertionError(f"Unexpected coroutine yield: {result!r}")

        async def fake_run_multi_crawl(args, *, project_root):
            captured_args["categories"] = args.categories
            captured_args["mode"] = args.mode
            captured_args["project_root"] = project_root
            return 0

        with patch.object(run_multi_crawl, "_run_multi_crawl", side_effect=fake_run_multi_crawl), patch.object(
            run_multi_crawl.asyncio, "run", side_effect=fake_asyncio_run
        ):
            exit_code = run_multi_crawl.main(["--categories", "bills", "members", "--mode", "incremental"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured_args["categories"], ["bills", "members"])
        self.assertEqual(captured_args["mode"], "incremental")
        self.assertEqual(captured_args["project_root"], PROJECT_ROOT)

    def test_multi_parse_args_supports_stale_cleanup_window(self) -> None:
        run_multi_crawl = _load_run_multi_crawl_module()

        args = run_multi_crawl.parse_args(["--cleanup-stale-hours", "6"], project_root=PROJECT_ROOT)

        self.assertEqual(args.cleanup_stale_hours, 6.0)


if __name__ == "__main__":
    unittest.main()
