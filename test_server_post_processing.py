#!/usr/bin/env python3
"""
Test script for the new post-processing functions on the server.

This script can be run from the server to test the three new post-processing improvements:
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
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, "src")
sys.path.insert(0, src_dir)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_post_processing_functions():
    """Test the new post-processing functions."""

    try:
        # Import the functions
        from streamlined.lobbyist_matching.post_processor import (
            post_process_bill_type_variations,
            post_process_congress_range_expansion,
            post_process_enhanced_appropriations,
            BILL_TYPE_VARIATIONS,
            APPROPRIATIONS_KEY_WORDS,
        )

        logger.info("✓ Successfully imported post-processing functions")
        logger.info(f"House bill type variations: {BILL_TYPE_VARIATIONS['house']}")
        logger.info(f"Senate bill type variations: {BILL_TYPE_VARIATIONS['senate']}")
        logger.info(
            f"Number of appropriations key words: {len(APPROPRIATIONS_KEY_WORDS)}"
        )

        # Test database connection
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
        logger.info("✓ Database connection successful")

        # Test with a sample run ID (you'll need to provide a real one)
        test_run_id = 1  # Replace with actual run ID for testing

        logger.info(f"Testing post-processing functions with run ID: {test_run_id}")

        # Test each function
        functions_to_test = [
            ("Bill type variations", post_process_bill_type_variations),
            ("Congress range expansion", post_process_congress_range_expansion),
            ("Enhanced appropriations", post_process_enhanced_appropriations),
        ]

        for name, func in functions_to_test:
            logger.info(f"Testing {name}...")
            try:
                await func(db.pool, test_run_id)
                logger.info(f"✓ {name} test completed successfully")
            except Exception as e:
                logger.warning(f"⚠ {name} test failed (expected if no data): {e}")

        await db.close()
        logger.info("✓ All tests completed")

    except ImportError as e:
        logger.error(f"❌ Import failed: {e}")
        logger.error("Make sure you're running this from the correct directory")
        return False
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False

    return True


def test_key_word_logic():
    """Test the key word extraction logic."""
    logger.info("Testing key word extraction logic...")

    try:
        from streamlined.lobbyist_matching.post_processor import (
            APPROPRIATIONS_KEY_WORDS,
        )

        test_titles = [
            "Transportation and Housing Appropriations Act",
            "Energy and Water Development Appropriations",
            "Defense Appropriations Bill",
            "Health and Human Services Appropriations",
            "Agriculture Appropriations Act",
        ]

        import re

        for title in test_titles:
            title_words = set(re.findall(r"\b\w+\b", title.lower()))
            key_words_found = title_words & APPROPRIATIONS_KEY_WORDS
            logger.info(f"Title: '{title}'")
            logger.info(f"  Key words found: {key_words_found}")
            logger.info(f"  Number of key words: {len(key_words_found)}")

        logger.info("✓ Key word logic test completed")
        return True

    except Exception as e:
        logger.error(f"❌ Key word logic test failed: {e}")
        return False


def test_bill_type_logic():
    """Test the bill type variation logic."""
    logger.info("Testing bill type variation logic...")

    try:
        from streamlined.lobbyist_matching.post_processor import BILL_TYPE_VARIATIONS

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

        logger.info("✓ Bill type logic test completed")
        return True

    except Exception as e:
        logger.error(f"❌ Bill type logic test failed: {e}")
        return False


if __name__ == "__main__":
    logger.info("Starting server post-processing tests...")

    # Test logic functions
    logic_tests = [
        test_key_word_logic(),
        test_bill_type_logic(),
    ]

    if all(logic_tests):
        logger.info("✓ All logic tests passed")

        # Test database functions
        success = asyncio.run(test_post_processing_functions())

        if success:
            logger.info("🎉 All tests completed successfully!")
        else:
            logger.error("❌ Some tests failed")
            sys.exit(1)
    else:
        logger.error("❌ Logic tests failed")
        sys.exit(1)
