#!/usr/bin/env python3
"""
Streamlined Dagster Migration Example

This example shows how to migrate existing Dagster assets from the old
ProcessingResource to the new StreamlinedProcessingResource, which provides
integrated run tracking, checkpointing, and the streamlined architecture.
"""

import asyncio
import logging
from typing import Any

from dagster import AssetExecutionContext, Definitions, asset

# Streamlined imports
from streamlined import (
    ResourceCoordinator,
    StreamlinedExecutor,
    StreamlinedFetcher,
    StreamlinedProcessingResource,
    get_streamlined_coordinator_from_context,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# OLD APPROACH (ProcessingResource)
# =============================================================================


@asset(
    name="bills_old_approach",
    group_name="old_pipeline",
    compute_kind="data_processing",
)
async def bills_pipeline_old_approach(
    context: AssetExecutionContext,
    processing_resource,  # Old ProcessingResource
) -> dict[str, Any]:
    """
    OLD: Using the old ProcessingResource approach.

    Problems:
    - Uses the complex 8-layer architecture
    - Relies on set_processing_resource() method
    - Less integrated run tracking and checkpointing
    """
    logger.info("=== OLD APPROACH: ProcessingResource ===")

    try:
        # Get database pool from old resource
        db_pool = await processing_resource.get_db_pool()

        # Create fetcher with old approach
        from streamlined.data_types.congressional.bills.fetcher import BillsFetcher

        fetcher = BillsFetcher(
            client=None,  # Would need to create separately
            db_pool=db_pool,
            data_type_name="bills",
        )

        # Attach processing resource (old pattern)
        fetcher.set_processing_resource(processing_resource)

        # Fetch some bills (old way)
        # This would use the complex 8-layer architecture
        logger.info("Old approach: Complex pipeline execution...")

        return {
            "approach": "old",
            "architecture_layers": 8,
            "resource_type": "ProcessingResource",
            "integration": "set_processing_resource",
            "bills_processed": 100,  # Mock data
        }

    except Exception as e:
        logger.error(f"Old approach failed: {e}")
        raise


# =============================================================================
# NEW APPROACH (StreamlinedProcessingResource)
# =============================================================================


@asset(
    name="bills_new_approach",
    group_name="streamlined_pipeline",
    compute_kind="streamlined_processing",
)
async def bills_pipeline_new_approach(
    context: AssetExecutionContext,
    processing_resource: StreamlinedProcessingResource,  # New streamlined resource
) -> dict[str, Any]:
    """
    NEW: Using the new StreamlinedProcessingResource.

    Benefits:
    - Uses the simplified 4-layer architecture
    - Direct ResourceCoordinator integration
    - Integrated run tracking and checkpointing
    - Focused resource managers
    """
    logger.info("=== NEW APPROACH: StreamlinedProcessingResource ===")

    try:
        # Get the ResourceCoordinator directly
        coordinator = await processing_resource.get_coordinator()

        # Create StreamlinedExecutor (new approach)
        executor = StreamlinedExecutor(coordinator)

        # Execute with the streamlined architecture
        results = await executor.execute_data_type(
            data_type="bills",
            phases=["raw", "staging"],  # Can control phases
            limit=100,
        )

        # The coordinator handles all resource management:
        # - Database connections
        # - Run tracking
        # - Checkpointing
        # - API key management
        # - Storage optimization

        logger.info("New approach: Streamlined pipeline execution complete!")

        return {
            "approach": "new_streamlined",
            "architecture_layers": 4,
            "resource_type": "StreamlinedProcessingResource",
            "integration": "ResourceCoordinator",
            "results": results,
            "bills_processed": results.get("total_processed", 100),
            "run_tracking": "integrated",
            "checkpointing": "hierarchical",
        }

    except Exception as e:
        logger.error(f"New approach failed: {e}")
        raise


# =============================================================================
# ALTERNATIVE NEW APPROACH (Direct ResourceCoordinator)
# =============================================================================


@asset(
    name="bills_direct_coordinator",
    group_name="streamlined_pipeline",
    compute_kind="direct_streamlined",
)
async def bills_pipeline_direct_coordinator(
    context: AssetExecutionContext,
    processing_resource: StreamlinedProcessingResource,
) -> dict[str, Any]:
    """
    ALTERNATIVE: Direct use of ResourceCoordinator for maximum control.

    Use this when you need fine-grained control over individual components.
    """
    logger.info("=== DIRECT COORDINATOR APPROACH ===")

    try:
        # Get coordinator using convenience function
        coordinator = await get_streamlined_coordinator_from_context(context)

        # Use individual components directly
        fetcher = StreamlinedFetcher(coordinator)

        # Access individual managers for fine control
        run_manager = await coordinator.run_manager.get_manager()
        checkpoint_manager = coordinator.checkpoint_manager.get_manager()

        # Create run for tracking
        from streamlined.libs.run_tracking import RunMetadata, RunType

        run_metadata = RunMetadata(
            run_id="",
            run_type=RunType.SCRAPER_CONGRESSIONAL,
            system_name="bills_direct_coordinator",
            description="Direct coordinator approach for bills processing",
            data_types=["bills"],
        )

        run_id = run_manager.create_run(run_metadata)
        await run_manager.start_run(run_id)

        try:
            # Process with direct component control
            # This gives you maximum flexibility
            logger.info(f"Processing bills with run_id: {run_id}")

            # Mock processing
            await asyncio.sleep(0.1)  # Simulate work

            # Complete the run
            await run_manager.complete_run(
                run_id,
                {
                    "bills_processed": 150,
                    "phases_completed": ["raw", "staging"],
                    "approach": "direct_coordinator",
                },
            )

            return {
                "approach": "direct_coordinator",
                "run_id": run_id,
                "run_tracking": "manual_control",
                "checkpointing": "available",
                "bills_processed": 150,
                "component_access": "direct",
            }

        except Exception as e:
            # Fail the run on error
            await run_manager.fail_run(run_id, str(e))
            raise

    except Exception as e:
        logger.error(f"Direct coordinator approach failed: {e}")
        raise


# =============================================================================
# MIGRATION UTILITY FUNCTIONS
# =============================================================================


async def compare_approaches():
    """Compare the different approaches for migration planning."""

    print("\n" + "=" * 80)
    print("STREAMLINED ARCHITECTURE MIGRATION COMPARISON")
    print("=" * 80)

    comparison = {
        "Old ProcessingResource": {
            "architecture_layers": 8,
            "complexity": "High",
            "run_tracking": "Basic",
            "checkpointing": "Limited",
            "resource_management": "God Object",
            "debugging": "Difficult",
            "performance": "Lower",
        },
        "New StreamlinedProcessingResource": {
            "architecture_layers": 4,
            "complexity": "Low",
            "run_tracking": "Integrated",
            "checkpointing": "Hierarchical",
            "resource_management": "Focused Managers",
            "debugging": "Easy",
            "performance": "Higher",
        },
    }

    for approach, metrics in comparison.items():
        print(f"\n{approach}:")
        for metric, value in metrics.items():
            print(f"  {metric:20}: {value}")

    print("\n" + "=" * 80)
    print("MIGRATION RECOMMENDATIONS")
    print("=" * 80)
    print("1. Update Dagster definitions to use StreamlinedProcessingResource")
    print("2. Migrate assets one by one (both approaches can coexist)")
    print("3. Use StreamlinedExecutor for end-to-end processing")
    print("4. Use direct ResourceCoordinator for fine-grained control")
    print("5. Test run tracking and checkpointing integration")
    print("=" * 80)


# =============================================================================
# DAGSTER DEFINITIONS
# =============================================================================


def create_migration_example_definitions():
    """Create Dagster definitions showing both old and new approaches."""

    # Import the old resource for comparison
    try:
        from src.bicam_collection.dagster_pipeline.shared_resources import (
            ProcessingResource,
        )

        old_resource = ProcessingResource()
    except ImportError:
        logger.warning("Old ProcessingResource not available for comparison")
        old_resource = None

    # Create new streamlined resource
    new_resource = StreamlinedProcessingResource()

    # Define resources
    resources = {
        "processing_resource": new_resource,  # Use new by default
    }

    # If we have the old resource, create a separate definition for comparison
    if old_resource:
        resources["old_processing_resource"] = old_resource

    return Definitions(
        assets=[
            bills_pipeline_new_approach,
            bills_pipeline_direct_coordinator,
            # Only include old approach if old resource is available
            bills_pipeline_old_approach if old_resource else None,
        ],
        resources=resources,
    )


# =============================================================================
# MAIN EXECUTION
# =============================================================================


async def main():
    """Run the migration comparison."""
    logger.info("Starting Streamlined Dagster Migration Example")

    # Show comparison
    await compare_approaches()

    # Note: In actual usage, these would be run by Dagster
    # This is just for demonstration of the concepts

    logger.info("\nMigration example complete!")
    logger.info("To use in Dagster:")
    logger.info("1. Replace ProcessingResource with StreamlinedProcessingResource")
    logger.info("2. Update asset functions to use the new resource")
    logger.info("3. Test with: dagster dev")


if __name__ == "__main__":
    asyncio.run(main())
