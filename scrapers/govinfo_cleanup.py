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

    # Commit the changes
    conn.commit()

    # Close the connection
    cur.close()


if __name__ == "__main__":
    main()