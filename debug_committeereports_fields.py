#!/usr/bin/env python3
"""
Debug script to check what fields are available in committee report data.
"""

import asyncio
import logging
from src.streamlined.resources.coordinator import ResourceCoordinator
from src.streamlined.fetcher import StreamlinedFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def debug_committeereports_fields():
    """Debug what fields are available in committee report data."""

    # Initialize coordinator with explicit source
    coordinator = ResourceCoordinator(source="congressional")
    await coordinator.initialize()

    # Create fetcher with explicit data source
    fetcher = await StreamlinedFetcher.from_coordinator(
        coordinator, data_type_name="committeereports", data_source="congressional"
    )

    try:
        # Check if we have a client
        if not fetcher.client:
            logger.error("No client available - trying to get it manually")
            clients = await coordinator.get_api_clients("committeereports")
            if clients:
                fetcher.client = clients[0]
                logger.info("Got client manually")
            else:
                logger.error("Could not get client")
                return

        # Get a single committee report to examine its structure
        logger.info("Fetching a single committee report to examine structure...")

        # Fetch one item
        async for batch in fetcher.fetch_phase_1_data_with_client(
            fetcher.client,
            from_date="2024-01-01",
            to_date="2024-01-31",
            limit=1,
            single_page_only=True,
        ):
            if batch:
                item = batch[0]
                logger.info(f"Found committee report item: {item.get('url', 'No URL')}")

                # Get the detailed data
                if url := item.get("url"):
                    detailed_data = await fetcher.fetch_phase_2_data_with_client(
                        url, fetcher.client
                    )
                    if detailed_data:
                        logger.info("=== COMMITTEE REPORT DATA STRUCTURE ===")
                        logger.info(f"Available fields: {list(detailed_data.keys())}")

                        # Look for text-related fields
                        text_fields = [
                            k for k in detailed_data.keys() if "text" in k.lower()
                        ]
                        logger.info(f"Text-related fields: {text_fields}")

                        # Show the structure of any text-related fields
                        for field in text_fields:
                            logger.info(f"Field '{field}': {detailed_data[field]}")

                        # Also check for any fields that might contain URLs
                        url_fields = []
                        for key, value in detailed_data.items():
                            if isinstance(value, dict) and "url" in value:
                                url_fields.append(key)
                        logger.info(f"Fields with URLs: {url_fields}")

                        # Show the structure of URL fields
                        for field in url_fields:
                            logger.info(f"URL field '{field}': {detailed_data[field]}")

                        break
                    else:
                        logger.error("Could not fetch detailed data")
                else:
                    logger.error("No URL in item")
                break
        else:
            logger.error("No committee reports found")

    except Exception as e:
        logger.error(f"Error during debug: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await coordinator.cleanup()


if __name__ == "__main__":
    asyncio.run(debug_committeereports_fields())
