import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

import asyncpg
import pytest

from bicam_collection.core.config import load_config
from bicam_collection.db.database_setup import DatabaseManager, setup_database
from bicam_collection.scrapers.bicam_govinfo import (
    DataProcessor as GovInfoDataProcessor,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def event_loop():
    """Create an instance of the default event loop for our test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def db_manager():
    """Create and setup a test database."""
    config = load_config()
    test_db_name = f"{config.database.database}_test"
    config.database.database = test_db_name

    await setup_database(config, recreate=True)

    manager = DatabaseManager(config)
    manager.pool = await asyncpg.create_pool(config.database.connection_string)

    yield manager

    await manager.pool.close()


class TestGovInfoScraperLastProcessed:
    """
    Test suite for GovInfo scraper logic related to `last_processed_date`.
    """

    @pytest.mark.asyncio
    async def test_new_bill_scraping(self, db_manager: DatabaseManager, mocker):
        """
        Test that new bills are scraped when no last_processed_date is set.
        """
        with open(FIXTURE_DIR / "new_bill.json") as f:
            new_bill_data = json.load(f)

        last_modified_date = new_bill_data[0]["last_modified"]

        mocker.patch.object(GovInfoDataProcessor, "get_data_stream", new_callable=AsyncMock, return_value=iter([new_bill_data]))

        processor = GovInfoDataProcessor(db_manager.config)
        await processor.run("bills")

        async with db_manager.pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT title FROM bicam_staging_govinfo.bills WHERE bill_id = $1",
                "hr-1234-117"
            )
            assert result is not None
            assert result["title"] == "Test Bill for New Scraping"

            last_processed = await conn.fetchval(
                "SELECT last_processed_date FROM bicam_metadata.govinfo_last_processed_dates WHERE data_type = 'bills'"
            )
            assert last_processed == last_modified_date

    @pytest.mark.asyncio
    async def test_updated_bill_scraping(self, db_manager: DatabaseManager, mocker):
        """
        Test that updated bills are re-scraped.
        """
        with open(FIXTURE_DIR / "updated_bill_old.json") as f:
            old_bill_data = json.load(f)[0]
        with open(FIXTURE_DIR / "updated_bill_new.json") as f:
            new_bill_data = json.load(f)

        new_last_modified = new_bill_data[0]["last_modified"]

        async with db_manager.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bicam_staging_govinfo.bills (bill_id, title, last_modified)
                VALUES ($1, $2, $3)
                """,
                old_bill_data["bill_id"],
                old_bill_data["title"],
                old_bill_data["last_modified"],
            )
            await conn.execute(
                """
                INSERT INTO bicam_metadata.govinfo_last_processed_dates (data_type, last_processed_date)
                VALUES ('bills', $1)
                """,
                old_bill_data["last_modified"]
            )

        mocker.patch.object(GovInfoDataProcessor, "get_data_stream", new_callable=AsyncMock, return_value=iter([new_bill_data]))

        processor = GovInfoDataProcessor(db_manager.config)
        await processor.run("bills")

        async with db_manager.pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT title FROM bicam_staging_govinfo.bills WHERE bill_id = $1",
                old_bill_data["bill_id"]
            )
            assert result is not None
            assert result["title"] == "Updated Test Bill Title"

            last_processed = await conn.fetchval(
                "SELECT last_processed_date FROM bicam_metadata.govinfo_last_processed_dates WHERE data_type = 'bills'"
            )
            assert last_processed == new_last_modified

    @pytest.mark.asyncio
    async def test_no_unnecessary_scraping(self, db_manager: DatabaseManager, mocker):
        """
        Test that bills are not re-scraped if they haven't been updated.
        """
        with open(FIXTURE_DIR / "updated_bill_old.json") as f:
            old_bill_data = json.load(f)[0]

        # This date is after the 'last_modified' date of the bill
        last_processed_date = "2023-10-28T10:00:00Z"

        async with db_manager.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bicam_staging_govinfo.bills (bill_id, title, last_modified)
                VALUES ($1, $2, $3)
                """,
                old_bill_data["bill_id"],
                old_bill_data["title"],
                old_bill_data["last_modified"],
            )
            await conn.execute(
                """
                INSERT INTO bicam_metadata.govinfo_last_processed_dates (data_type, last_processed_date)
                VALUES ('bills', $1)
                """,
                last_processed_date
            )

        # The scraper should not return any new data
        mocker.patch.object(GovInfoDataProcessor, "get_data_stream", new_callable=AsyncMock, return_value=iter([]))

        processor = GovInfoDataProcessor(db_manager.config)
        await processor.run("bills")

        async with db_manager.pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT title FROM bicam_staging_govinfo.bills WHERE bill_id = $1",
                old_bill_data["bill_id"]
            )
            assert result is not None
            # Title should NOT have changed
            assert result["title"] == old_bill_data["title"]

            last_processed = await conn.fetchval(
                "SELECT last_processed_date FROM bicam_metadata.govinfo_last_processed_dates WHERE data_type = 'bills'"
            )
            # last_processed_date should NOT have changed
            assert last_processed == last_processed_date
