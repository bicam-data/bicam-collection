"""Pytest configuration and common fixtures."""

import asyncio
import os
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from typing import AsyncGenerator, Generator

# Ensure test environment variables
os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("DEBUG", "true")


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def mock_db_pool() -> AsyncGenerator[AsyncMock, None]:
    """Mock database connection pool."""
    pool = AsyncMock()
    
    # Mock connection context manager
    conn = AsyncMock()
    pool.acquire.return_value.__aenter__.return_value = conn
    pool.acquire.return_value.__aexit__.return_value = None
    
    yield pool


@pytest.fixture
def mock_congress_api() -> AsyncMock:
    """Mock Congressional API client."""
    api = AsyncMock()
    api.get_bulk_bills.return_value = []
    api.get_bulk_committees.return_value = []
    api.get_bulk_members.return_value = []
    return api


@pytest.fixture
def mock_govinfo_api() -> AsyncMock:
    """Mock GovInfo API client.""" 
    api = AsyncMock()
    api.get_collections.return_value = []
    return api


@pytest.fixture
def sample_bill_data() -> dict:
    """Sample bill data for testing."""
    return {
        "bill_id": "hr1234-118",
        "bill_type": "hr",
        "bill_number": "1234", 
        "congress": 118,
        "title": "Test Bill Title",
        "introduced_date": "2023-01-01",
        "latest_action": {
            "action_date": "2023-01-15",
            "text": "Referred to committee"
        }
    }


@pytest.fixture
def sample_committee_data() -> dict:
    """Sample committee data for testing."""
    return {
        "committee_id": "hsag00",
        "name": "House Committee on Agriculture",
        "chamber": "house",
        "committee_type": "standing"
    }


@pytest.fixture
def mock_settings() -> MagicMock:
    """Mock application settings."""
    settings = MagicMock()
    settings.database.host = "localhost"
    settings.database.port = 5432
    settings.database.database = "test_bicam"
    settings.database.user = "test_user" 
    settings.database.password = "test_pass"
    settings.database.dsn = "postgresql://test_user:test_pass@localhost:5432/test_bicam"
    
    settings.api.congress_api_keys = ["test_key_1", "test_key_2"]
    settings.api.govinfo_api_key = "test_govinfo_key"
    settings.api.rate_limit = 5
    settings.api.max_retries = 2
    
    settings.processing.batch_size = 10
    settings.processing.max_workers = 2
    settings.processing.chunk_size = 100
    
    return settings


@pytest.fixture
def temp_csv_file(tmp_path):
    """Create a temporary CSV file for testing."""
    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text(
        "id,name,value\n"
        "1,test1,100\n"
        "2,test2,200\n"
    )
    return str(csv_file)


# Test markers for different test types
pytest.mark.unit = pytest.mark.unit
pytest.mark.integration = pytest.mark.integration  
pytest.mark.slow = pytest.mark.slow 