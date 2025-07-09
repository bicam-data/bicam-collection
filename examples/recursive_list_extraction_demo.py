"""
Demo of Recursive List Extraction in StreamlinedNormalizer

This demonstrates how the normalizer can extract lists from:
1. Main tables (e.g., amendments)
2. Related tables (e.g., amendments_actions)
3. Extracted list tables (e.g., amendments_actions_recordedvotes)

Example hierarchy:
- amendments (main table)
  - amendments_actions (extracted from amendments.actions)
    - amendments_actions_recordedvotes (extracted from amendments_actions.recordedvotes)
    - amendments_actions_committees (extracted from amendments_actions.committees)
  - amendments_cosponsors (extracted from amendments.cosponsors)
    - amendments_cosponsors_links (extracted from amendments_cosponsors.links)
"""

import asyncio
import json
import logging
from datetime import UTC, datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demo_recursive_extraction():
    """Demonstrate recursive list extraction with sample data."""

    # Sample amendment data with nested lists
    sample_amendment = {
        "amendment_id": "samdt123-118",
        "congress": 118,
        "chamber": "senate",
        "actions": [
            {
                "action_id": "act1",
                "action_date": "2024-01-15",
                "text": "Introduced",
                "recordedvotes": [
                    {"roll_number": 1, "date": "2024-01-16", "result": "Passed"},
                    {"roll_number": 2, "date": "2024-01-17", "result": "Failed"},
                ],
                "committees": [
                    {
                        "committee_code": "SSJU",
                        "committee_name": "Senate Judiciary Committee",
                    }
                ],
            },
            {
                "action_id": "act2",
                "action_date": "2024-01-20",
                "text": "Reported",
                "recordedvotes": [],
                "committees": [],
            },
        ],
        "cosponsors": [
            {
                "cosponsor_id": "S001234",
                "name": "Senator Smith",
                "links": [{"url": "https://example.com/smith", "type": "website"}],
            },
            {
                "cosponsor_id": "S005678",
                "name": "Senator Jones",
                "links": [
                    {"url": "https://example.com/jones", "type": "website"},
                    {"url": "https://example.com/jones/bio", "type": "biography"},
                ],
            },
        ],
    }

    logger.info("Sample amendment data structure:")
    logger.info(f"- Main table: amendments")
    logger.info(f"- Extracted lists: actions, cosponsors")
    logger.info(
        f"- Nested lists: actions.recordedvotes, actions.committees, cosponsors.links"
    )

    # Show the extraction hierarchy
    extraction_hierarchy = {
        "amendments": {
            "description": "Main amendment table",
            "extracted_lists": ["actions", "cosponsors"],
            "nested_tables": {
                "amendments_actions": {
                    "description": "Extracted from amendments.actions",
                    "extracted_lists": ["recordedvotes", "committees"],
                    "nested_tables": {
                        "amendments_actions_recordedvotes": {
                            "description": "Extracted from amendments_actions.recordedvotes"
                        },
                        "amendments_actions_committees": {
                            "description": "Extracted from amendments_actions.committees"
                        },
                    },
                },
                "amendments_cosponsors": {
                    "description": "Extracted from amendments.cosponsors",
                    "extracted_lists": ["links"],
                    "nested_tables": {
                        "amendments_cosponsors_links": {
                            "description": "Extracted from amendments_cosponsors.links"
                        }
                    },
                },
            },
        }
    }

    logger.info("\nExtraction hierarchy:")
    _print_hierarchy(extraction_hierarchy, level=0)

    # Show what the normalizer would do
    logger.info("\nNormalizer processing phases:")
    logger.info("Phase 1: Extract main amendment data to amendments table")
    logger.info("Phase 2: Extract actions and cosponsors to separate tables")
    logger.info("Phase 3: Extract nested lists (recordedvotes, committees, links)")
    logger.info("Phase 4: Clean up JSON columns from all tables")

    # Show sample table structures
    logger.info("\nSample table structures:")

    logger.info("\n1. amendments (main table):")
    logger.info("   - amendment_id TEXT")
    logger.info("   - congress TEXT")
    logger.info("   - chamber TEXT")
    logger.info("   - actions TEXT (JSON array - will be extracted)")
    logger.info("   - cosponsors TEXT (JSON array - will be extracted)")

    logger.info("\n2. amendments_actions (extracted from amendments.actions):")
    logger.info("   - id TEXT")
    logger.info("   - amendment_id TEXT")
    logger.info("   - action_id TEXT")
    logger.info("   - action_date TEXT")
    logger.info("   - text TEXT")
    logger.info("   - recordedvotes TEXT (JSON array - will be extracted)")
    logger.info("   - committees TEXT (JSON array - will be extracted)")

    logger.info(
        "\n3. amendments_actions_recordedvotes (extracted from amendments_actions.recordedvotes):"
    )
    logger.info("   - id TEXT")
    logger.info("   - amendment_id TEXT")
    logger.info("   - action_id TEXT")
    logger.info("   - roll_number TEXT")
    logger.info("   - date TEXT")
    logger.info("   - result TEXT")

    logger.info(
        "\n4. amendments_cosponsors_links (extracted from amendments_cosponsors.links):"
    )
    logger.info("   - id TEXT")
    logger.info("   - amendment_id TEXT")
    logger.info("   - cosponsor_id TEXT")
    logger.info("   - url TEXT")
    logger.info("   - type TEXT")

    logger.info("\nKey benefits of recursive extraction:")
    logger.info("1. Handles complex nested data structures")
    logger.info("2. Creates normalized tables for all list data")
    logger.info("3. Maintains referential integrity with foreign keys")
    logger.info("4. Enables efficient querying of nested data")
    logger.info("5. Automatically discovers and processes new nested lists")


def _print_hierarchy(hierarchy, level=0):
    """Print the extraction hierarchy in a tree format."""
    indent = "  " * level

    for table_name, details in hierarchy.items():
        logger.info(f"{indent}- {table_name}")
        if "description" in details:
            logger.info(f"{indent}  Description: {details['description']}")
        if "extracted_lists" in details:
            logger.info(
                f"{indent}  Extracted lists: {', '.join(details['extracted_lists'])}"
            )
        if "nested_tables" in details:
            logger.info(f"{indent}  Nested tables:")
            _print_hierarchy(details["nested_tables"], level + 2)


if __name__ == "__main__":
    asyncio.run(demo_recursive_extraction())
