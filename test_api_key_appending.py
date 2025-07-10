#!/usr/bin/env python3
"""
Test script to verify API keys are being properly appended to URLs.

This script tests the Congressional API client to ensure API keys are correctly
added to request URLs.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from streamlined.api_clients.congressional_api import CongressionalAPIClient
from streamlined.resources.config import get_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


async def test_api_key_appending():
    """Test that API keys are being properly appended to URLs."""

    # Load configuration
    config = get_config()

    # Get API keys
    api_keys = config.get("congressional_api_keys", [])
    if not api_keys:
        logger.error("No Congressional API keys found in configuration")
        return False

    logger.info(f"Testing with {len(api_keys)} API keys")
    logger.info(f"First API key: {api_keys[0][:10]}...")

    # Create API client
    client = CongressionalAPIClient(api_keys=api_keys)

    try:
        # Test a simple endpoint that should work
        logger.info("Testing API key appending with a simple endpoint...")

        # Test the _make_request method directly to see the URL construction
        logger.info("Testing URL construction...")

        # Create a mock session to capture the URL
        import aiohttp
        from unittest.mock import AsyncMock, MagicMock

        # Mock the session to capture the URL
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"test": "data"})
        mock_response.text = AsyncMock(return_value='{"test": "data"}')

        mock_session = MagicMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session.close = AsyncMock()

        # Replace the session in the client
        client.session = mock_session

        # Test the _make_request method
        result = await client._make_request("/bill/116/hr/1")

        # Check what URL was actually called
        call_args = mock_session.get.call_args
        if call_args:
            url = call_args[0][0]  # First positional argument
            logger.info(f"URL that was called: {url}")

            # Check if API key is in the URL
            if "api_key=" in url:
                logger.info("✅ API key found in URL!")
                # Extract the API key from the URL
                import urllib.parse

                parsed = urllib.parse.urlparse(url)
                query_params = urllib.parse.parse_qs(parsed.query)
                api_key_in_url = query_params.get("api_key", [None])[0]
                if api_key_in_url:
                    logger.info(f"API key in URL: {api_key_in_url[:10]}...")
                    if api_key_in_url == api_keys[0]:
                        logger.info("✅ API key matches the first key in our list!")
                    else:
                        logger.warning("❌ API key in URL doesn't match our first key!")
                else:
                    logger.warning("❌ No api_key parameter found in URL!")
            else:
                logger.error("❌ No api_key parameter found in URL!")
                logger.error(f"Full URL: {url}")
        else:
            logger.error("❌ No URL was called!")

        # Close the mock session
        await mock_session.close()

        # Now test with a real request to see the actual response
        logger.info("Testing with a real API request...")

        # Create a real session
        async with aiohttp.ClientSession() as session:
            client.session = session

            try:
                # Test a simple endpoint
                result = await client._make_request("/bill/116/hr/1")
                logger.info("✅ Real API request succeeded!")
                logger.info(
                    f"Response keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}"
                )
            except Exception as e:
                logger.error(f"❌ Real API request failed: {e}")
                return False

        return True

    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        if hasattr(client, "session") and client.session:
            await client.session.close()


async def test_api_key_rotation():
    """Test that API key rotation is working properly."""

    # Load configuration
    config = get_config()

    # Get API keys
    api_keys = config.get("congressional_api_keys", [])
    if not api_keys:
        logger.error("No Congressional API keys found in configuration")
        return False

    logger.info(f"Testing API key rotation with {len(api_keys)} keys...")

    # Create API client
    client = CongressionalAPIClient(api_keys=api_keys)

    try:
        # Test initial key
        initial_key = client._get_current_api_key()
        logger.info(f"Initial API key: {initial_key[:10]}...")

        # Test rotation
        for i in range(min(3, len(api_keys))):
            client._rotate_api_key()
            new_key = client._get_current_api_key()
            logger.info(f"After rotation {i + 1}: {new_key[:10]}...")

            if new_key != initial_key:
                logger.info("✅ API key rotation working!")
                break
        else:
            logger.warning("⚠️ API key rotation might not be working as expected")

        return True

    except Exception as e:
        logger.error(f"API key rotation test failed: {e}")
        return False


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("TESTING API KEY APPENDING")
    logger.info("=" * 60)

    success1 = asyncio.run(test_api_key_appending())

    logger.info("=" * 60)
    logger.info("TESTING API KEY ROTATION")
    logger.info("=" * 60)

    success2 = asyncio.run(test_api_key_rotation())

    if success1 and success2:
        logger.info("✅ All tests passed!")
        sys.exit(0)
    else:
        logger.error("❌ Some tests failed!")
        sys.exit(1)
