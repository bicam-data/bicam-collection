#!/usr/bin/env python3
"""
Bicam Collection CLI

A modular command-line interface for the bicam-collection data pipeline.
This script provides commands for database setup, data extraction, cleaning,
merging, exporting, and lobbyist matching.
"""

import argparse
import asyncio
import logging
import sys

# Third-party imports
import structlog

# Local imports
from bicam_collection.core.checkpoint_manager import CheckpointManager
from bicam_collection.core.config import create_sample_config, load_config
from bicam_collection.core.schema_config import SchemaManager
from bicam_collection.db.database_setup import DatabaseManager, setup_database
from bicam_collection.normalization.normalizer import normalize_data_type
from bicam_collection.scrapers.bicam_congressional import (
    DataProcessor as CongressionalDataProcessor,
)
from bicam_collection.scrapers.bicam_govinfo import (
    DataProcessor as GovInfoDataProcessor,
)

# Setup structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class BicamCLI:
    """Main CLI class for bicam-collection operations"""

    def __init__(self, config_path: str | None = None):
        """Initialize CLI with configuration"""
        try:
            self.config = load_config(config_path)
            self.db_manager = DatabaseManager(self.config)
            self.checkpoint_manager = CheckpointManager()
            self.schema_manager = SchemaManager()
            logger.info(
                "Configuration loaded successfully", environment=self.config.environment
            )
        except Exception as e:
            logger.error("Failed to load configuration", error=str(e))
            sys.exit(1)

    async def setup_database(self, recreate: bool = False) -> bool:
        """Set up the database schemas and tables"""
        logger.info("Starting database setup", recreate=recreate)

        try:
            success = await setup_database(self.config, recreate)
            if success:
                logger.info("Database setup completed successfully")
            else:
                logger.error("Database setup failed")
            return success
        except Exception as e:
            logger.error("Database setup error", error=str(e))
            return False

    async def validate_database(self) -> bool:
        """Validate database setup"""
        logger.info("Validating database setup")

        try:
            validation_results = await self.db_manager.validate_database_setup()

            # Print validation results
            for check, result in validation_results.items():
                status = "✓" if result else "✗"
                logger.info(f"  {status} {check.replace('_', ' ').title()}")

            success = all(validation_results.values())
            if success:
                logger.info("Database validation passed")
            else:
                logger.error("Database validation failed")

            return success
        except Exception as e:
            logger.error("Database validation error", error=str(e))
            return False

    async def run_scraper(self, scraper_type: str, **kwargs) -> bool:
        """Run data extraction/scraping (supports pass-through flags)."""
        logger.info("Starting data extraction", scraper_type=scraper_type)

        try:
            if scraper_type == "bicam_congressional":
                return await self._run_congressional_scraper(**kwargs)
            elif scraper_type == "bicam_govinfo":
                return await self._run_govinfo_scraper(**kwargs)
            else:
                logger.error("Unknown scraper type", scraper_type=scraper_type)
                return False
        except Exception as e:
            logger.error("Scraper error", scraper_type=scraper_type, error=str(e))
            return False

    async def _run_congressional_scraper(
        self,
        data_types: list[str] | None = None,
        output: str | None = None,
        reset_checkpoint: str | None = None,
        clear_progress: bool = False,
        show_progress: bool = False,
        sync_dates: bool = False,
        env: str | None = None,
        write_csv: bool = False,
        **__,
    ) -> bool:
        """Run congressional data scraper.

        The underlying DataProcessor now expects ``requested_types`` – a list of
        data-type strings – rather than the legacy ``specific_data_type``
        parameter.  We simply forward the list supplied via the CLI (or *None*
        for "all main=true").
        """

        processor = CongressionalDataProcessor(
            config_path=self.config.congressional_config_path,
            output_dir=(output or self.config.scraping.output_directory)
            if write_csv
            else None,
            env_path=env or ".env",
        )

        # ------------------------------------------------------------
        # Handle maintenance/utility operations first (no main run)
        # ------------------------------------------------------------
        if show_progress:
            summary = processor.checkpoint_manager.get_progress_summary("congressional")
            logger.info("Progress Summary: %s", summary)
            for cp in processor.checkpoint_manager.list_checkpoints("congressional"):
                tr = processor.get_hierarchical_tracker(cp.data_type)
                logger.info("%s: %s", cp.data_type, tr.detailed_progress)
            return True

        if reset_checkpoint:
            processor.checkpoint_manager.reset_checkpoint(
                "congressional", reset_checkpoint
            )
            logger.info("Reset checkpoint for %s", reset_checkpoint)
            return True

        if sync_dates:
            await processor.initialize()
            await processor.sync_last_processed_dates()
            await processor.cleanup()
            logger.info("Synced last processed dates")
            return True

        if clear_progress:
            if data_types:
                for dt in data_types:
                    processor.checkpoint_manager.reset_checkpoint("congressional", dt)
                logger.info("Cleared progress for %s", ", ".join(data_types))
            else:
                for cp in processor.checkpoint_manager.list_checkpoints(
                    "congressional"
                ):
                    processor.checkpoint_manager.reset_checkpoint(
                        "congressional", cp.data_type
                    )
                logger.info("Cleared all progress")
            return True

        # ------------------------------------------------------------
        # Normal run
        # ------------------------------------------------------------
        await processor.run(requested_types=data_types)
        return True

    async def _run_govinfo_scraper(
        self,
        data_types: list[str] | None = None,
        output: str | None = None,
        reset_checkpoint: str | None = None,
        clear_progress: bool = False,
        env: str | None = None,
        write_csv: bool = False,
        **__,
    ) -> bool:
        """Run GovInfo data scraper (updated signature)."""

        processor = GovInfoDataProcessor(
            config_path=self.config.govinfo_config_path,
            output_dir=(output or self.config.scraping.output_directory)
            if write_csv
            else None,
            env_path=env or ".env",
        )

        if reset_checkpoint:
            processor.checkpoint_manager.reset_checkpoint("govinfo", reset_checkpoint)
            logger.info("Reset checkpoint for %s", reset_checkpoint)
            return True

        if clear_progress:
            if data_types:
                for dt in data_types:
                    processor.checkpoint_manager.reset_checkpoint("govinfo", dt)
                logger.info("Cleared progress for %s", ", ".join(data_types))
            else:
                for cp in processor.checkpoint_manager.list_checkpoints("govinfo"):
                    processor.checkpoint_manager.reset_checkpoint(
                        "govinfo", cp.data_type
                    )
                logger.info("Cleared all progress (govinfo)")
            return True

        await processor.run(requested_types=data_types)
        return True

    def run_cleaning(self, data_types: list[str] | None = None) -> bool:
        """Run data cleaning process"""
        logger.info("Starting data cleaning", data_types=data_types)

        try:
            # This will need to be updated to call a main function
            # from a cleaning coordinator module.
            # For now, this is a placeholder.
            logger.warning("run_cleaning is not fully implemented after refactor")
            return False
        except Exception as e:
            logger.error("Cleaning error", error=str(e))
            return False

    def run_merging(self) -> bool:
        """Run data merging process"""
        logger.info("Starting data merging")

        try:
            # This will need to be updated to call a main function
            # from a merging module.
            # For now, this is a placeholder.
            logger.warning("run_merging is not fully implemented after refactor")
            return False
        except Exception as e:
            logger.error("Merging error", error=str(e))
            return False

    def run_export(self, export_format: str | None = None) -> bool:
        """Run data export process"""
        logger.info("Starting data export", format=export_format)

        try:
            # This will need to be updated to call a main function
            # from an exporter module.
            # For now, this is a placeholder.
            logger.warning("run_export is not fully implemented after refactor")
            return False
        except Exception as e:
            logger.error("Export error", error=str(e))
            return False

    def run_lobbyist_matching(self) -> bool:
        """Run lobbyist matching process"""
        logger.info("Starting lobbyist matching")

        try:
            # This will need to be updated to call a main function
            # from a lobbyist matching module.
            # For now, this is a placeholder.
            logger.warning(
                "run_lobbyist_matching is not fully implemented after refactor"
            )
            return False
        except Exception as e:
            logger.error("Lobbyist matching error", error=str(e))
            return False

    def run_normalization(self, data_types: list[str] | None = None) -> bool:
        """Run normalization for given data types (raw → intermediate)."""
        if not data_types:
            logger.error("No data types specified for normalization")
            return False

        logger.info("Starting normalization", data_types=data_types)
        try:
            for dt in data_types:
                try:
                    normalize_data_type(dt)
                except Exception as e:
                    logger.error("Normalization failed", data_type=dt, error=str(e))
                    return False
            logger.info("Normalization completed successfully")
            return True
        except Exception as e:
            logger.error("Normalization error", error=str(e))
            return False

    async def run_full_pipeline(self, skip_setup: bool = False) -> bool:
        """Run the full data pipeline from setup to export"""
        logger.info("Starting full data pipeline")

        # 1. Setup Database
        if not skip_setup and not await self.setup_database():
            logger.error("Full pipeline failed: Database setup error")
            return False

        # 2. Run Scrapers
        scrapers_to_run = ["bicam_congressional", "bicam_govinfo"]
        scraper_tasks = [self.run_scraper(s) for s in scrapers_to_run]
        results = await asyncio.gather(*scraper_tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception) or not result:
                logger.error(
                    "Full pipeline failed: Scraper error", scraper=scrapers_to_run[i]
                )
                return False

        # 3. Run Cleaning
        if not self.run_cleaning():
            logger.error("Full pipeline failed: Cleaning error")
            return False

        # 4. Run Merging
        if not self.run_merging():
            logger.error("Full pipeline failed: Merging error")
            return False

        # 5. Run Lobbyist Matching
        if not self.run_lobbyist_matching():
            logger.error("Full pipeline failed: Lobbyist matching error")
            return False

        # 6. Run Export
        if not self.run_export():
            logger.error("Full pipeline failed: Export error")
            return False

        logger.info("Full data pipeline completed successfully")
        return True

    def show_status(self) -> None:
        """Show the current status of the data pipeline"""
        logger.info("Showing pipeline status")
        asyncio.run(self._show_database_status())
        self.checkpoint_manager.display_all_checkpoints()

    async def _show_database_status(self) -> None:
        """Show database status"""
        logger.info("\nDatabase Status:")

        # Test connection
        connection_ok = await self.db_manager.test_connection()
        logger.info(f"  Connection: {'✓' if connection_ok else '✗'}")

        if connection_ok:
            # Get database info
            db_info = await self.db_manager.get_database_info()
            for key, value in db_info.items():
                logger.info(f"  {key.replace('_', ' ').title()}: {value}")

            # Get schemas
            schemas = await self.db_manager.get_existing_schemas()
            logger.info(f"  Schemas: {', '.join(sorted(schemas))}")


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser"""
    parser = argparse.ArgumentParser(
        description="Bicam Collection Data Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--config", "-c", type=str, help="Path to configuration file (YAML)"
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="func", help="Available commands")

    # Config commands
    config_parser = subparsers.add_parser("config", help="Configuration management")
    config_subparsers = config_parser.add_subparsers(dest="config_command")

    config_subparsers.add_parser(
        "create-sample", help="Create sample configuration file"
    )
    config_subparsers.add_parser("validate", help="Validate configuration")

    # Database commands
    db_parser = subparsers.add_parser("database", help="Database management")
    db_subparsers = db_parser.add_subparsers(
        dest="database_command", help="Database commands"
    )

    setup_parser = db_subparsers.add_parser(
        "setup", help="Setup database schemas and tables"
    )
    setup_parser.add_argument(
        "--recreate", action="store_true", help="Recreate the database from scratch"
    )
    validate_parser = db_subparsers.add_parser(
        "validate", help="Validate database setup"
    )

    # Scraper Parser
    scraper_parser = subparsers.add_parser("scraper", help="Run data scrapers")
    scraper_parser.add_argument(
        "scraper_type",
        choices=["bicam_congressional", "bicam_govinfo"],
        help="Type of scraper to run",
    )
    scraper_parser.add_argument(
        "--data-types", nargs="+", help="Specific data types to scrape"
    )
    scraper_parser.add_argument(
        "--output", type=str, help="Override output directory for this scraper run"
    )
    scraper_parser.add_argument(
        "--reset-checkpoint",
        help="Reset checkpoint for a specific data type before running",
    )
    scraper_parser.add_argument(
        "--clear-progress",
        action="store_true",
        help="Clear progress tracking for all or selected data types",
    )
    scraper_parser.add_argument(
        "--show-progress", action="store_true", help="Show checkpoint progress and exit"
    )
    scraper_parser.add_argument(
        "--sync-dates",
        action="store_true",
        help="Sync last processed dates to metadata table and exit",
    )
    scraper_parser.add_argument(
        "--env",
        type=str,
        help="Path to .env with DB creds/API keys (overrides project default)",
    )
    scraper_parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write intermediate CSV files (default off)",
    )

    # Processing commands
    clean_parser = subparsers.add_parser("clean", help="Data cleaning")
    clean_parser.add_argument(
        "--data-types", nargs="+", help="Specific data types to clean"
    )

    subparsers.add_parser("merge", help="Data merging")

    export_parser = subparsers.add_parser("export", help="Data export")
    export_parser.add_argument(
        "--format", choices=["csv", "parquet", "json", "xlsx"], help="Export format"
    )

    subparsers.add_parser("lobbyist-matching", help="Lobbyist matching")

    # Pipeline commands
    pipeline_parser = subparsers.add_parser("pipeline", help="Full pipeline operations")
    pipeline_parser.add_argument(
        "--skip-setup", action="store_true", help="Skip database setup"
    )

    # Status command
    status_parser = subparsers.add_parser("status", help="Show pipeline status")
    status_parser.add_argument(
        "--check-db", action="store_true", help="Check database connection and schema"
    )

    run_parser = subparsers.add_parser("run", help="Run a specific pipeline component")
    run_parser.add_argument(
        "component",
        choices=[
            "scraper",
            "normalization",
            "cleaning",
            "merging",
            "export",
            "lobbyist_matching",
            "full_pipeline",
        ],
    )
    run_parser.add_argument(
        "--scraper-type",
        choices=["bicam_congressional", "bicam_govinfo"],
        help="Specify scraper type",
    )
    run_parser.add_argument(
        "--data-types", nargs="+", help="Specify data types for scraper or cleaning"
    )
    run_parser.add_argument(
        "--export-format", help="Specify export format (e.g., csv, parquet)"
    )
    run_parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip database setup in full pipeline run",
    )
    run_parser.add_argument(
        "--output", type=str, help="Override output directory when component is scraper"
    )
    run_parser.add_argument(
        "--reset-checkpoint",
        help="Reset checkpoint for a specific data type (scraper component)",
    )
    run_parser.add_argument(
        "--clear-progress",
        action="store_true",
        help="Clear progress tracking when component is scraper",
    )
    run_parser.add_argument(
        "--show-progress",
        action="store_true",
        help="Show progress and exit (scraper component)",
    )
    run_parser.add_argument(
        "--sync-dates",
        action="store_true",
        help="Sync last processed dates (scraper component)",
    )
    run_parser.add_argument(
        "--env", type=str, help="Override .env path for scraper component"
    )
    run_parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write intermediate CSV files (default off)",
    )

    return parser


async def main():
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    # Initialize CLI
    try:
        cli = BicamCLI(args.config)
    except Exception as e:
        logger.error("Failed to initialize CLI", error=str(e))
        sys.exit(1)

    # Handle commands
    success = True

    if args.func == "database":
        if args.database_command == "setup":
            success = await cli.setup_database(recreate=args.recreate)
        elif args.database_command == "validate":
            success = await cli.validate_database()
        elif args.database_command == "status":
            cli.show_status()
    elif args.func == "setup":
        success = await cli.setup_database(recreate=args.recreate)
    elif args.func == "validate":
        success = await cli.validate_database()
    elif args.func == "scraper":
        success = await cli.run_scraper(
            scraper_type=args.scraper_type,
            data_types=args.data_types,
            output=getattr(args, "output", None),
            reset_checkpoint=getattr(args, "reset_checkpoint", None),
            clear_progress=getattr(args, "clear_progress", False),
            show_progress=getattr(args, "show_progress", False),
            sync_dates=getattr(args, "sync_dates", False),
            env=getattr(args, "env", None),
            write_csv=getattr(args, "write_csv", False),
        )
    elif args.func == "run":
        # Versatile "run" shortcut that supports multiple pipeline components
        if args.component == "scraper":
            if not args.scraper_type:
                logger.error("Scraper type must be specified with --scraper-type")
                sys.exit(1)

            success = await cli.run_scraper(
                scraper_type=args.scraper_type,
                data_types=args.data_types,
                output=getattr(args, "output", None),
                reset_checkpoint=getattr(args, "reset_checkpoint", None),
                clear_progress=getattr(args, "clear_progress", False),
                show_progress=getattr(args, "show_progress", False),
                sync_dates=getattr(args, "sync_dates", False),
                env=getattr(args, "env", None),
                write_csv=getattr(args, "write_csv", False),
            )
        elif args.component == "cleaning":
            success = cli.run_cleaning(data_types=args.data_types)
        elif args.component == "normalization":
            success = cli.run_normalization(data_types=args.data_types)
        elif args.component == "merging":
            success = cli.run_merging()
        elif args.component == "export":
            success = cli.run_export(export_format=args.export_format)
        elif args.component == "lobbyist_matching":
            success = cli.run_lobbyist_matching()
        elif args.component == "full_pipeline":
            success = await cli.run_full_pipeline(skip_setup=args.skip_setup)
    elif args.func == "cleaning":
        success = cli.run_cleaning(data_types=args.data_types)
    elif args.func == "normalization":
        success = cli.run_normalization(data_types=args.data_types)
    elif args.func == "merging":
        success = cli.run_merging()
    elif args.func == "export":
        success = cli.run_export(export_format=args.export_format)
    elif args.func == "lobbyist_matching":
        success = cli.run_lobbyist_matching()
    elif args.func == "full_pipeline":
        success = await cli.run_full_pipeline(skip_setup=args.skip_setup)
    elif args.func == "status":
        cli.show_status()
    elif args.func == "config":
        create_sample_config()

    sys.exit(0 if success else 1)


def cli_main():
    """Synchronous wrapper for the main async function."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("CLI process interrupted by user.")
        sys.exit(0)


if __name__ == "__main__":
    cli_main()
