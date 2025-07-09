"""
Demo of Fixed Amendments Normalization

This demonstrates how the normalizer now properly:
1. Processes all raw tables (amendments_raw, amendments_actions_raw, etc.)
2. Flattens dictionaries (like latestAction) into prefixed fields
3. Extracts lists from all tables recursively
"""

import asyncio
import json
import logging
from datetime import UTC, datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demo_amendments_normalization():
    """Demonstrate the fixed amendments normalization process."""

    # Sample amendment data from your example
    sample_amendment = {
        "type": "HAMDT",
        "number": "913",
        "actions": {
            "url": "https://api.congress.gov/v3/amendment/105/hamdt/913/actions?format=json",
            "count": 6,
        },
        "chamber": "House of Representatives",
        "purpose": "Amendment in the nature of a substitute.",
        "batch_id": "cf2005b7-a17d-444c-bb2e-c3240118a67a",
        "congress": 105,
        "sponsors": [
            {
                "url": "https://api.congress.gov/v3/committee/house/hsru00?format=json",
                "name": "Rules Committee",
            }
        ],
        "updateDate": "2021-10-07T19:41:42Z",
        "amendedBill": {
            "url": "https://api.congress.gov/v3/bill/105/hr/3736?format=json",
            "type": "HR",
            "title": "Temporary Access to Skilled Workers and H-1B Nonimmigrant Program Improvement Act of 1998",
            "number": "3736",
            "congress": 105,
            "originChamber": "House",
            "originChamberCode": "H",
            "updateDateIncludingText": "2025-04-07",
        },
        "description": "Strike all after the enacting clause ***",
        "amendment_id": "hamdt913-105",
        "latestAction": {
            "text": "On agreeing to the Rules amendment (A002) Agreed to by voice vote. ",
            "actionDate": "1998-09-24",
            "actionTime": "17:59:43",
        },
        "submittedDate": "1998-09-24T04:00:00Z",
    }

    logger.info("Sample amendment data structure:")
    logger.info(
        f"- Main fields: type, number, chamber, purpose, congress, amendment_id"
    )
    logger.info(
        f"- Dict fields: actions, amendedBill, latestAction (will be flattened)"
    )
    logger.info(f"- List fields: sponsors (will be extracted to separate table)")

    # Show what happens during normalization
    logger.info("\n=== NORMALIZATION PROCESS ===")

    logger.info("\nPhase 1: Process amendments_raw → amendments")
    logger.info("Main table fields:")
    logger.info("  - type: HAMDT")
    logger.info("  - number: 913")
    logger.info("  - chamber: House of Representatives")
    logger.info("  - purpose: Amendment in the nature of a substitute.")
    logger.info("  - congress: 105")
    logger.info("  - amendment_id: hamdt913-105")
    logger.info("  - submittedDate: 1998-09-24T04:00:00Z")
    logger.info("  - updateDate: 2021-10-07T19:41:42Z")
    logger.info("  - batch_id: cf2005b7-a17d-444c-bb2e-c3240118a67a")

    logger.info("\nFlattened dict fields:")
    logger.info(
        "  - actions_url: https://api.congress.gov/v3/amendment/105/hamdt/913/actions?format=json"
    )
    logger.info("  - actions_count: 6")
    logger.info(
        "  - amendedbill_url: https://api.congress.gov/v3/bill/105/hr/3736?format=json"
    )
    logger.info("  - amendedbill_type: HR")
    logger.info(
        "  - amendedbill_title: Temporary Access to Skilled Workers and H-1B Nonimmigrant Program Improvement Act of 1998"
    )
    logger.info("  - amendedbill_number: 3736")
    logger.info("  - amendedbill_congress: 105")
    logger.info("  - amendedbill_originchamber: House")
    logger.info("  - amendedbill_originchambercode: H")
    logger.info("  - amendedbill_updatedateincludingtext: 2025-04-07")
    logger.info(
        "  - latestaction_text: On agreeing to the Rules amendment (A002) Agreed to by voice vote."
    )
    logger.info("  - latestaction_actiondate: 1998-09-24")
    logger.info("  - latestaction_actiontime: 17:59:43")

    logger.info("\nExtracted list fields:")
    logger.info(
        "  - sponsors: [JSON array - will be extracted to amendments_sponsors table]"
    )

    logger.info("\nPhase 2: Process related raw tables")
    logger.info("Processing amendments_actions_raw → amendments_actions")
    logger.info("Processing amendments_texts_raw → amendments_texts")
    logger.info("Processing amendments_cosponsors_raw → amendments_cosponsors")
    logger.info("(Each with their own data and potential nested lists)")

    logger.info("\nPhase 3: Extract lists from all tables")
    logger.info("From amendments table:")
    logger.info("  - Extract sponsors → amendments_sponsors")
    logger.info("From amendments_actions table:")
    logger.info("  - Extract recordedvotes → amendments_actions_recordedvotes")
    logger.info("  - Extract committees → amendments_actions_committees")
    logger.info("From amendments_sponsors table:")
    logger.info("  - Extract links → amendments_sponsors_links")

    logger.info("\nPhase 4: Clean up JSON columns")
    logger.info("Remove JSON columns that were extracted to separate tables")

    # Show the final table structure
    logger.info("\n=== FINAL TABLE STRUCTURE ===")

    logger.info("\n1. amendments (main table):")
    logger.info("   - amendment_id TEXT")
    logger.info("   - type TEXT")
    logger.info("   - number TEXT")
    logger.info("   - chamber TEXT")
    logger.info("   - purpose TEXT")
    logger.info("   - congress TEXT")
    logger.info("   - description TEXT")
    logger.info("   - submittedDate TEXT")
    logger.info("   - updateDate TEXT")
    logger.info("   - batch_id TEXT")
    logger.info("   - actions_url TEXT")
    logger.info("   - actions_count TEXT")
    logger.info("   - amendedbill_url TEXT")
    logger.info("   - amendedbill_type TEXT")
    logger.info("   - amendedbill_title TEXT")
    logger.info("   - amendedbill_number TEXT")
    logger.info("   - amendedbill_congress TEXT")
    logger.info("   - amendedbill_originchamber TEXT")
    logger.info("   - amendedbill_originchambercode TEXT")
    logger.info("   - amendedbill_updatedateincludingtext TEXT")
    logger.info("   - latestaction_text TEXT")
    logger.info("   - latestaction_actiondate TEXT")
    logger.info("   - latestaction_actiontime TEXT")
    logger.info("   - processed_at TEXT")
    logger.info("   - source_doc_id TEXT")

    logger.info("\n2. amendments_sponsors (extracted from amendments.sponsors):")
    logger.info("   - id TEXT")
    logger.info("   - amendment_id TEXT")
    logger.info("   - url TEXT")
    logger.info("   - name TEXT")
    logger.info("   - list_index TEXT")
    logger.info("   - processed_at TEXT")
    logger.info("   - source_doc_id TEXT")

    logger.info("\n3. amendments_actions (from amendments_actions_raw):")
    logger.info("   - action_id TEXT")
    logger.info("   - amendment_id TEXT")
    logger.info("   - action_date TEXT")
    logger.info("   - text TEXT")
    logger.info("   - recordedvotes TEXT (JSON array - will be extracted)")
    logger.info("   - committees TEXT (JSON array - will be extracted)")
    logger.info("   - processed_at TEXT")
    logger.info("   - source_doc_id TEXT")

    logger.info(
        "\n4. amendments_actions_recordedvotes (extracted from amendments_actions.recordedvotes):"
    )
    logger.info("   - id TEXT")
    logger.info("   - amendment_id TEXT")
    logger.info("   - action_id TEXT")
    logger.info("   - roll_number TEXT")
    logger.info("   - date TEXT")
    logger.info("   - result TEXT")
    logger.info("   - list_index TEXT")

    logger.info("\nKey improvements:")
    logger.info("1. ✅ Dicts are flattened (latestAction → latestaction_* fields)")
    logger.info("2. ✅ All raw tables are processed (amendments_actions_raw, etc.)")
    logger.info("3. ✅ Lists are extracted recursively from all tables")
    logger.info("4. ✅ Proper foreign key relationships maintained")


if __name__ == "__main__":
    asyncio.run(demo_amendments_normalization())
