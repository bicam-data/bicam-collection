import psycopg2 as pg
import os
from dotenv import load_dotenv
import subprocess

load_dotenv()
def main():


    command = "rm -r /mnt/big_data/database-congress/api-congress/*"
    try:
        subprocess.run(command, shell=True)
    except Exception as e:
        pass
    # Connect to the database
    conn = pg.connect(
        dbname=os.getenv("POSTGRESQL_DB"),
        user=os.getenv("POSTGRESQL_USER"),
        password=os.getenv("POSTGRESQL_PASSWORD"),
        host=os.getenv("POSTGRESQL_HOST"),
        port=os.getenv("POSTGRESQL_PORT"))

    cur = conn.cursor()

    # Drop the tables
    cur.execute("""
                drop table if exists _sandbox_ryan.committeemeetings;

    drop table if exists _sandbox_ryan.committeemeetings_witnesses;

    drop table if exists _sandbox_ryan.committeemeetings_associated_bill_ids;

    drop table if exists _sandbox_ryan.committeemeetings_witness_documents;

    drop table if exists _sandbox_ryan.committeemeetings_meeting_documents;

    drop table if exists _sandbox_ryan.committeeprints_associated_bill_ids;

    drop table if exists _sandbox_ryan.congresses;

    drop table if exists _sandbox_ryan.congresses_sessions;

    drop table if exists _sandbox_ryan.committees;

    drop table if exists _sandbox_ryan.committees_subcommittee_codes;

    drop table if exists _sandbox_ryan.committees_history;

    drop table if exists _sandbox_ryan.hearings;

    drop table if exists _sandbox_ryan.hearings_committee_codes;

    drop table if exists _sandbox_ryan.hearings_dates;

    drop table if exists _sandbox_ryan.hearings_formats;

    drop table if exists _sandbox_ryan.committeereports;

    drop table if exists _sandbox_ryan.committeeprints;

    drop table if exists _sandbox_ryan.committeeprints_committee_codes;

    drop table if exists _sandbox_ryan.members;

    drop table if exists _sandbox_ryan.members_terms;

    drop table if exists _sandbox_ryan.members_leadership_roles;

    drop table if exists _sandbox_ryan.treaties;

    drop table if exists _sandbox_ryan.treaties_index_terms;

    drop table if exists _sandbox_ryan.treaties_country_parties;

    drop table if exists _sandbox_ryan.treaties_titles;

    drop table if exists _sandbox_ryan.nominations;

    drop table if exists _sandbox_ryan.nominations_positions;

    drop table if exists _sandbox_ryan.amendments;

    drop table if exists _sandbox_ryan.bills;

    drop table if exists _sandbox_ryan.bills_cbocostestimates;

    drop table if exists _sandbox_ryan.bills_sponsors;

    drop table if exists _sandbox_ryan.bills_committeereports;

    drop table if exists _sandbox_ryan.committeemeetings_associated_nomination_ids;

    drop table if exists _sandbox_ryan.committeereports_texts;

    drop table if exists _sandbox_ryan.nominations_actions;

    drop table if exists _sandbox_ryan.nominations_actions_committee_codes;

    drop table if exists _sandbox_ryan.amendments_actions;

    drop table if exists _sandbox_ryan.bills_actions;

    drop table if exists _sandbox_ryan.bills_actions_committee_codes;

    drop table if exists _sandbox_ryan.bills_actions_recorded_votes;

    drop table if exists _sandbox_ryan.bills_titles;

    drop table if exists _sandbox_ryan.bills_committeeactivities;

    drop table if exists _sandbox_ryan.bills_cosponsors;

    drop table if exists _sandbox_ryan.bills_billrelations;

    drop table if exists _sandbox_ryan.bills_texts;

    drop table if exists _sandbox_ryan.amendments_cosponsors;

    drop table if exists _sandbox_ryan.amendments_texts;

    drop table if exists _sandbox_ryan.treaties_actions;

    drop table if exists _sandbox_ryan.nominations_committeeactivities;

    drop table if exists _sandbox_ryan.nominations_nominees;

    drop table if exists _sandbox_ryan.nominations_hearings;

    drop table if exists _sandbox_ryan.bills_summaries;

    drop table if exists _sandbox_ryan.bills_subjects;

    drop table if exists _sandbox_ryan.committeeprints_texts;

    drop table if exists _sandbox_ryan.committees_bills;

    drop table if exists _sandbox_ryan.committees_relatedreports;

    drop table if exists _sandbox_ryan.amendments_actions_recorded_votes;

    drop table if exists _sandbox_ryan.amendments_sponsor_bioguide_ids;


                """)

    # truncate every table in the _staging_congressional schema
    # cur.execute("""
    #     SELECT table_name
    #     FROM information_schema.tables
    #     WHERE table_schema = '_staging_congressional'
    #     AND table_type = 'BASE TABLE';
    # """)
    # staging_tables = cur.fetchall()
    # if staging_tables is not None:
    #     for table in staging_tables:
    #         cur.execute(f"truncate table _staging_congressional.{table[0]}")
    # else:
    #     pass

    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'congressional'
        AND table_type = 'BASE TABLE';
    """)
    prod_tables = cur.fetchall()
    if prod_tables is not None:
        for table in prod_tables:
            cur.execute(f"truncate table congressional.{table[0]}")
    else:
        pass

    # set all values in last_processed_dates to 1789-01-01
    cur.execute("""
        update __metadata.last_processed_dates
        set last_processed_date = '1789-01-01'
    """)
    
    cur.execute("""
        truncate table __metadata.congressional_errors;
    """)

    # Commit the changes
    conn.commit()

    # Close the connection
    cur.close()

if __name__ == "__main__":
    main()