"""
Demo of Fixed Table Processing Logic

This demonstrates how the normalizer now properly:
1. Checks raw tables first before looking in staging
2. Processes all related tables that have raw data
3. Handles the correct order of operations
"""

import asyncio
import json
import logging
from datetime import UTC, datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demo_fixed_table_processing():
    """Demonstrate the fixed table processing logic."""

    logger.info("=== FIXED TABLE PROCESSING LOGIC ===")

    logger.info("\nProblem with old logic:")
    logger.info("❌ Checked if 'amendments_actions' exists in STAGING")
    logger.info("❌ 'amendments_actions' doesn't exist in staging yet (not processed)")
    logger.info(
        "❌ Skipped processing even though 'amendments_actions_raw' exists in RAW"
    )

    logger.info("\nFixed logic:")
    logger.info("✅ Check if 'amendments_actions_raw' exists in RAW schema")
    logger.info(
        "✅ If raw table exists, check if 'amendments_actions' exists in STAGING"
    )
    logger.info("✅ If staging table exists, process it for list extraction")
    logger.info("✅ If staging table doesn't exist, log warning (Phase 2 issue)")

    logger.info("\n=== PROCESSING FLOW ===")

    logger.info("\nPhase 1: Process main table")
    logger.info("amendments_raw → amendments")
    logger.info("- Extract lists: sponsors → amendments_sponsors")
    logger.info("- Flatten dicts: latestAction → latestaction_* fields")

    logger.info("\nPhase 2: Process related raw tables")
    logger.info("Check each related table in config.related_tables:")

    related_tables = [
        "sponsors",
        "cosponsors",
        "on_behalf_of",
        "links",
        "amended_bills",
        "amended_treaties",
        "amended_amendments",
        "actions",
        "texts",
    ]

    for rt in related_tables:
        raw_table = f"amendments_{rt}_raw"
        staging_table = f"amendments_{rt}"

        if rt in ["actions", "texts", "cosponsors"]:
            logger.info(f"✅ {raw_table} exists in RAW → Process {staging_table}")
        elif rt in [
            "on_behalf_of",
            "links",
            "amended_bills",
            "amended_treaties",
            "amended_amendments",
        ]:
            logger.info(f"❌ {raw_table} does not exist in RAW → Skip {staging_table}")
        else:
            logger.info(f"❓ {raw_table} - depends on create_raw setting")

    logger.info("\nPhase 3: Extract lists from all processed tables")
    logger.info("For each table that exists in staging:")
    logger.info("- amendments: Extract sponsors → amendments_sponsors")
    logger.info("- amendments_actions: Extract recordedvotes, committees")
    logger.info("- amendments_texts: Extract versions, formats")
    logger.info("- amendments_cosponsors: Extract links")

    logger.info("\nPhase 4: Clean up JSON columns")
    logger.info("Remove JSON columns that were extracted to separate tables")

    logger.info("\n=== KEY IMPROVEMENTS ===")
    logger.info("1. ✅ Checks raw schema first, not staging")
    logger.info("2. ✅ Processes all tables that have raw data")
    logger.info("3. ✅ Proper error messages when raw exists but staging doesn't")
    logger.info("4. ✅ Maintains correct processing order")

    logger.info("\n=== EXPECTED LOG MESSAGES ===")
    logger.info(
        "✅ 'Found existing staging table amendments_actions, will process for list extraction'"
    )
    logger.info(
        "✅ 'Found existing staging table amendments_texts, will process for list extraction'"
    )
    logger.info(
        "✅ 'Found existing staging table amendments_cosponsors, will process for list extraction'"
    )
    logger.info(
        "❌ 'Skipping list extraction for amendments_on_behalf_of - raw table bicam_raw.amendments_on_behalf_of_raw does not exist'"
    )


if __name__ == "__main__":
    asyncio.run(demo_fixed_table_processing())
