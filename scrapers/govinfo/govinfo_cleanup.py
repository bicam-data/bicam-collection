import psycopg2 as pg
import os
from dotenv import load_dotenv
import subprocess

load_dotenv()
def main():


    command = "rm -r /mnt/big_data/database-congress/api-govinfo/*"
    try:
        subprocess.run(command, shell=True)
    except Exception as e:
        pass
    conn = pg.connect(
        dbname=os.getenv("POSTGRESQL_DB"),
        user=os.getenv("POSTGRESQL_USER"),
        password=os.getenv("POSTGRESQL_PASSWORD"),
        host=os.getenv("POSTGRESQL_HOST"),
        port=os.getenv("POSTGRESQL_PORT"))

    cur = conn.cursor()
    cur.execute("""
        update __metadata.govinfo_last_processed_dates
        set last_processed_date = '1789-01-01'
    """)

        # Connect to the database
    conn = pg.connect(
        dbname=os.getenv("POSTGRESQL_DB"),
        user=os.getenv("POSTGRESQL_USER"),
        password=os.getenv("POSTGRESQL_PASSWORD"),
        host=os.getenv("POSTGRESQL_HOST"),
        port=os.getenv("POSTGRESQL_PORT"))

    cur = conn.cursor()

    # Get all tables in congressional schema and truncate them
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '_staging_govinfo'
        AND table_type = 'BASE TABLE';
    """)
    prod_tables = cur.fetchall()
    if prod_tables is not None:
        for table in prod_tables:
            cur.execute(f"truncate table _staging_govinfo.{table[0]}")
    else:
        pass
     # Clear error tracking table
    cur.execute("""
        truncate table __metadata.govinfo_errors;
    """)

    # Commit the changes
    conn.commit()

    # Close the connection
    cur.close()


if __name__ == "__main__":
    main()