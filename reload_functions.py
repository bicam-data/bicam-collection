#!/usr/bin/env python3
"""
Reload SQL functions programmatically
"""

import logging
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def reload_functions():
    """Reload the SQL functions from the updated file"""
    try:
        # Read the SQL file
        with open("convert_amendments_json_to_sql.sql") as f:
            sql_content = f.read()

        # Connect to database
        conn = psycopg2.connect(
            host=os.getenv("POSTGRESQL_HOST"),
            database=os.getenv("POSTGRESQL_DATABASE"),
            user=os.getenv("POSTGRESQL_USERNAME"),
            password=os.getenv("POSTGRESQL_PASSWORD"),
        )
        conn.autocommit = True

        logger.info("Database connection established")

        # Execute the SQL to reload functions
        with conn.cursor() as cursor:
            cursor.execute(sql_content)
            logger.info("Functions reloaded successfully")

        # Verify the function was updated
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT pg_get_functiondef(oid) 
                FROM pg_proc 
                WHERE proname = 'extract_amendment_data'
                LIMIT 1;
            """)
            result = cursor.fetchone()
            if result:
                function_def = result[0]
                if "json_data->>'latestAction'->>'text'" in function_def:
                    logger.error("Function still contains the old problematic code!")
                else:
                    logger.info(
                        "Function successfully updated - no longer contains problematic latestAction access"
                    )
            else:
                logger.error("Function not found!")

        conn.close()
        logger.info("Database connection closed")

    except Exception as e:
        logger.error(f"Error reloading functions: {e}")
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    reload_functions()
