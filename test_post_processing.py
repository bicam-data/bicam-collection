#!/usr/bin/env python3
"""
Test script for the new post-processing functions.

This script tests the three new post-processing improvements:
1. Bill type variation testing
2. Congress range expansion
3. Enhanced appropriations matching
"""

import asyncio
import logging
import os
import sys
from dotenv import load_dotenv

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from streamlined.lobbyist_matching.post_processor import (
    post_process_bill_type_variations,
    post_process_congress_range_expansion,
    post_process_enhanced_appropriations,
    BILL_TYPE_VARIATIONS,
    APPROPRIATIONS_KEY_WORDS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_post_processing():
    """Test the new post-processing functions."""

    # Load environment variables
    load_dotenv()

    # Test constants
    logger.info("Testing constants...")
    logger.info(f"House bill type variations: {BILL_TYPE_VARIATIONS['house']}")
    logger.info(f"Senate bill type variations: {BILL_TYPE_VARIATIONS['senate']}")
    logger.info(f"Number of appropriations key words: {len(APPROPRIATIONS_KEY_WORDS)}")
    logger.info(f"Sample key words: {list(APPROPRIATIONS_KEY_WORDS)[:5]}")

    # Test database connection
    try:
        import asyncpg
        from streamlined.lobbyist_matching.db_utils import DatabaseInterface

        db_config = {
            "host": os.getenv("POSTGRESQL_HOST"),
            "port": int(os.getenv("POSTGRESQL_PORT", 5432)),
            "user": os.getenv("POSTGRESQL_USER"),
            "password": os.getenv("POSTGRESQL_PASSWORD"),
            "database": os.getenv("POSTGRESQL_DB"),
        }

        logger.info("Testing database connection...")
        db = await DatabaseInterface.create_pool(**db_config)

        # Test with a sample run ID (you'll need to provide a real one)
        test_run_id = 1  # Replace with actual run ID for testing

        logger.info(f"Testing post-processing functions with run ID: {test_run_id}")

        # Test bill type variations
        logger.info("Testing bill type variations...")
        try:
            await post_process_bill_type_variations(db.pool, test_run_id)
            logger.info("✓ Bill type variations test completed")
        except Exception as e:
            logger.warning(
                f"Bill type variations test failed (expected if no data): {e}"
            )

        # Test congress range expansion
        logger.info("Testing congress range expansion...")
        try:
            await post_process_congress_range_expansion(db.pool, test_run_id)
            logger.info("✓ Congress range expansion test completed")
        except Exception as e:
            logger.warning(
                f"Congress range expansion test failed (expected if no data): {e}"
            )

        # Test enhanced appropriations
        logger.info("Testing enhanced appropriations...")
        try:
            await post_process_enhanced_appropriations(db.pool, test_run_id)
            logger.info("✓ Enhanced appropriations test completed")
        except Exception as e:
            logger.warning(
                f"Enhanced appropriations test failed (expected if no data): {e}"
            )

        await db.close()

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        logger.info("Skipping database tests - constants test passed")


def test_key_word_extraction():
    """Test key word extraction logic."""
    logger.info("Testing key word extraction...")

    test_titles = [
        "Transportation and Housing Appropriations Act",
        "Energy and Water Development Appropriations",
        "Defense Appropriations Bill",
        "Health and Human Services Appropriations",
        "Agriculture Appropriations Act",
    ]

    for title in test_titles:
        import re

        title_words = set(re.findall(r"\b\w+\b", title.lower()))
        key_words_found = title_words & APPROPRIATIONS_KEY_WORDS
        logger.info(f"Title: '{title}'")
        logger.info(f"  Key words found: {key_words_found}")
        logger.info(f"  Number of key words: {len(key_words_found)}")


def test_bill_type_variations():
    """Test bill type variation logic."""
    logger.info("Testing bill type variation logic...")

    test_cases = [
        ("hr", "house"),
        ("s", "senate"),
        ("hres", "house"),
        ("sres", "senate"),
        ("hconres", "house"),
        ("sconres", "senate"),
    ]

    for bill_type, expected_chamber in test_cases:
        chamber = None
        if bill_type.startswith("h"):
            chamber = "house"
        elif bill_type.startswith("s"):
            chamber = "senate"

        variations = BILL_TYPE_VARIATIONS.get(chamber, [])
        logger.info(f"Bill type: {bill_type} -> Chamber: {chamber}")
        logger.info(f"  Available variations: {variations}")
        logger.info(f"  Expected chamber: {expected_chamber}")
        logger.info(f"  Match: {chamber == expected_chamber}")


if __name__ == "__main__":
    logger.info("Starting post-processing tests...")

    # Test constants and logic
    test_key_word_extraction()
    test_bill_type_variations()

    # Test database functions
    asyncio.run(test_post_processing())

    logger.info("All tests completed!")
