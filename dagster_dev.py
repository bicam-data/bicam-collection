#!/usr/bin/env python3
"""
Dagster Development Entry Point for bicam-collection

This script provides a simple way to run the Dagster pipeline during Phase 1
of the migration from the heavyweight orchestrators to Dagster.

Usage:
    python dagster_dev.py                    # Start Dagster UI
    python dagster_dev.py --materialize bills # Materialize bills asset
    python dagster_dev.py --job bills_pipeline # Run bills pipeline job
    python dagster_dev.py --validate         # Validate pipeline definitions
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dagster import DagsterInstance, materialize

# Add src to path for development
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_dagster_definitions():
    """Get the Dagster definitions for development."""
    try:
        from bicam_collection.dagster_pipeline.definitions import defs

        return defs
    except ImportError as e:
        logger.error(f"Error importing Dagster definitions: {e}")
        logger.error(
            "Make sure you're running from the project root and dependencies are installed."
        )
        sys.exit(1)


def validate_pipeline():
    """Validate the Dagster pipeline definitions."""
    logger.info("🔍 Validating Dagster pipeline definitions...")

    try:
        defs = get_dagster_definitions()

        # Basic validation - check that definitions object exists and has key attributes
        if not hasattr(defs, "assets"):
            logger.error("❌ Definitions object missing 'assets' attribute")
            return False

        if not hasattr(defs, "jobs"):
            logger.error("❌ Definitions object missing 'jobs' attribute")
            return False

        # Count assets
        asset_count = len(defs.assets or [])
        logger.info(f"✅ Found {asset_count} assets")

        # Count jobs
        job_count = len(defs.jobs or [])
        logger.info(f"✅ Found {job_count} jobs")

        # List job names if available
        if defs.jobs:
            for job in defs.jobs:
                job_name = getattr(job, "name", getattr(job, "_name", "unnamed"))
                logger.info(f"   - {job_name}")

        # Count schedules
        schedule_count = len(defs.schedules or [])
        logger.info(f"✅ Found {schedule_count} schedules")

        # List schedule names if available
        if defs.schedules:
            for schedule in defs.schedules:
                schedule_name = getattr(
                    schedule, "name", getattr(schedule, "_name", "unnamed")
                )
                logger.info(f"   - {schedule_name}")

        # Count sensors
        sensor_count = len(defs.sensors or [])
        logger.info(f"✅ Found {sensor_count} sensors")

        # List sensor names if available
        if defs.sensors:
            for sensor in defs.sensors:
                sensor_name = getattr(
                    sensor, "name", getattr(sensor, "_name", "unnamed")
                )
                logger.info(f"   - {sensor_name}")

        logger.info("✅ Pipeline validation completed successfully!")
        logger.info(
            f"\n📊 Summary: {asset_count} assets, {job_count} jobs, {schedule_count} schedules, {sensor_count} sensors"
        )
        return True

    except Exception as e:
        logger.error(f"❌ Pipeline validation failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def materialize_asset(asset_name: str):
    """Materialize a specific asset."""
    logger.info(f"🏗️  Materializing asset: {asset_name}")

    try:
        defs = get_dagster_definitions()

        # Get available asset names from the definitions - handle multi-assets
        available_assets = []
        target_asset = None

        if defs.assets:
            for asset in defs.assets:
                # Handle multi-assets which have multiple keys
                try:
                    # Try single asset first
                    if hasattr(asset, "key"):
                        # Extract the path from AssetKey
                        asset_key = (
                            asset.key.path[-1]
                            if hasattr(asset.key, "path")
                            else str(asset.key)
                        )
                        available_assets.append(asset_key)
                        if asset_key == asset_name:
                            target_asset = asset
                except Exception:
                    # Handle multi-assets
                    if hasattr(asset, "keys"):
                        for key in asset.keys:
                            # Extract the path from AssetKey
                            asset_key = (
                                key.path[-1] if hasattr(key, "path") else str(key)
                            )
                            available_assets.append(asset_key)
                            if asset_key == asset_name:
                                target_asset = asset

        if asset_name not in available_assets:
            logger.error(f"❌ Asset '{asset_name}' not found. Available assets:")
            for asset in available_assets:
                logger.error(f"   - {asset}")
            return False

        # Use development instance
        instance = DagsterInstance.ephemeral()

        if target_asset:
            # For multi-assets, we can materialize specific keys
            if hasattr(target_asset, "keys") and len(target_asset.keys) > 1:
                # Multi-asset case - materialize just the specific asset key
                from dagster import AssetKey

                result = materialize(
                    [AssetKey(asset_name)], instance=instance, resources=defs.resources
                )
            else:
                # Single asset case
                result = materialize(
                    [target_asset], instance=instance, resources=defs.resources
                )

            if result.success:
                logger.info(f"✅ Successfully materialized {asset_name}")
                return True
            else:
                logger.error(f"❌ Failed to materialize {asset_name}")
                return False
        else:
            logger.error(f"❌ Could not find asset {asset_name}")
            return False

    except Exception as e:
        logger.error(f"❌ Error materializing asset: {e}")
        import traceback

        traceback.print_exc()
        return False


def run_job(job_name: str):
    """Run a specific job."""
    logger.info(f"🚀 Running job: {job_name}")

    try:
        defs = get_dagster_definitions()

        # Get available job names from the definitions
        available_jobs = []
        target_job = None

        if defs.jobs:
            for job in defs.jobs:
                job_name_attr = getattr(job, "name", getattr(job, "_name", None))
                if job_name_attr:
                    available_jobs.append(job_name_attr)
                    if job_name_attr == job_name:
                        target_job = job

        if job_name not in available_jobs:
            logger.error(f"❌ Job '{job_name}' not found. Available jobs:")
            for job in available_jobs:
                logger.error(f"   - {job}")
            return False

        if not target_job:
            logger.error(f"❌ Could not find job {job_name}")
            return False

        # Use development instance
        instance = DagsterInstance.ephemeral()

        # Execute the job
        result = target_job.execute_in_process(instance=instance)

        if result.success:
            logger.info(f"✅ Successfully completed job {job_name}")
            return True
        else:
            logger.error(f"❌ Job {job_name} failed")
            return False

    except Exception as e:
        logger.error(f"❌ Error running job: {e}")
        import traceback

        traceback.print_exc()
        return False


def start_dagster_ui():
    """Start the Dagster UI for development."""
    logger.info("🌐 Starting Dagster UI...")
    logger.info("📍 The UI will be available at: http://localhost:3000")
    logger.info("🛑 Press Ctrl+C to stop")

    try:
        # Set up environment for Dagster UI
        os.environ["DAGSTER_HOME"] = str(Path.cwd() / ".dagster_home")

        # Create dagster home if it doesn't exist
        dagster_home = Path(os.environ["DAGSTER_HOME"])
        dagster_home.mkdir(exist_ok=True)

        # Write dagster.yaml if it doesn't exist
        dagster_yaml = dagster_home / "dagster.yaml"
        if not dagster_yaml.exists():
            dagster_yaml.write_text("""
run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher

run_storage:
  module: dagster.core.storage.runs
  class: SqliteRunStorage
  config:
    base_dir: .dagster_home/storage

event_log_storage:
  module: dagster.core.storage.event_log
  class: SqliteEventLogStorage
  config:
    base_dir: .dagster_home/storage

schedule_storage:
  module: dagster.core.storage.schedules
  class: SqliteScheduleStorage
  config:
    base_dir: .dagster_home/storage
""")

        # Use subprocess to run dagster dev command
        import subprocess

        definitions_module = "bicam_collection.dagster_pipeline.definitions"

        cmd = ["dagster", "dev", "-m", definitions_module, "--port", "3000"]

        logger.info(f"Running command: {' '.join(cmd)}")

        # Run the command
        subprocess.run(cmd, check=True)

    except KeyboardInterrupt:
        logger.info("\n👋 Dagster UI stopped")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Error starting Dagster UI: {e}")
        logger.error(
            "Make sure Dagster is properly installed and the definitions module is accessible"
        )
    except Exception as e:
        logger.error(f"❌ Error starting Dagster UI: {e}")
        import traceback

        traceback.print_exc()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dagster Development Entry Point for bicam-collection"
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--validate", action="store_true", help="Validate pipeline definitions"
    )
    group.add_argument("--materialize", type=str, help="Materialize a specific asset")
    group.add_argument("--job", type=str, help="Run a specific job")
    group.add_argument(
        "--ui",
        action="store_true",
        help="Start Dagster UI (default if no other option specified)",
    )

    args = parser.parse_args()

    # Default to UI if no specific action
    if not any([args.validate, args.materialize, args.job, args.ui]):
        args.ui = True

    logger.info("🚀 Bicam Collection - Dagster Phase 1 Development")
    logger.info("=" * 50)

    if args.validate:
        success = validate_pipeline()
        sys.exit(0 if success else 1)

    elif args.materialize:
        success = materialize_asset(args.materialize)
        sys.exit(0 if success else 1)

    elif args.job:
        success = run_job(args.job)
        sys.exit(0 if success else 1)

    elif args.ui:
        start_dagster_ui()


if __name__ == "__main__":
    main()
