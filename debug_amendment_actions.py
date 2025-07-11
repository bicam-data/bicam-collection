#!/usr/bin/env python3
"""
Debug script to investigate amendment_actions processing issues
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Add the current directory to the path so we can import the processor
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from process_amendments import GenericRawTableProcessor


def debug_amendment_actions():
    """Debug amendment_actions processing"""

    # Database configuration
    db_config = {
        "host": os.getenv("POSTGRESQL_HOST"),
        "port": os.getenv("POSTGRESQL_PORT"),
        "database": os.getenv("POSTGRESQL_DATABASE"),
        "user": os.getenv("POSTGRESQL_USERNAME"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
    }

    processor = GenericRawTableProcessor(db_config)

    try:
        # Connect to database
        processor.connect()

        # Check statistics first
        print("=== Amendment Actions Statistics ===")
        stats = processor.get_processing_stats("amendment_actions")
        for key, value in stats.items():
            print(f"{key}: {value}")

        # Debug the raw data
        print("\n=== Raw Data Investigation ===")
        with processor.connection.cursor() as cursor:
            # Check total counts
            cursor.execute(
                "SELECT COUNT(*) FROM bicam_raw_congressional.amendments_raw"
            )
            total_amendments = cursor.fetchone()[0]
            print(f"Total amendments in raw table: {total_amendments}")

            cursor.execute(
                "SELECT COUNT(*) FROM bicam_raw_congressional.amendments_actions_raw"
            )
            total_actions = cursor.fetchone()[0]
            print(f"Total actions in raw table: {total_actions}")

            # Check amendments with recent updates
            cursor.execute("""
                SELECT COUNT(*) FROM bicam_raw_congressional.amendments_raw 
                WHERE (payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE
            """)
            recent_amendments = cursor.fetchone()[0]
            print(f"Amendments with recent updates: {recent_amendments}")

            # Check the join between amendments and actions
            cursor.execute("""
                SELECT COUNT(*) FROM bicam_raw_congressional.amendments_actions_raw a 
                INNER JOIN bicam_raw_congressional.amendments_raw am ON a.source_doc_id = am.source_doc_id 
                WHERE (am.payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE
            """)
            matching_actions = cursor.fetchone()[0]
            print(f"Actions matching recent amendments: {matching_actions}")

            # Check a few sample records
            print("\n=== Sample Records ===")

            # Sample amendment with recent update
            cursor.execute("""
                SELECT source_doc_id, payload->>'updateDate' as update_date 
                FROM bicam_raw_congressional.amendments_raw 
                WHERE (payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE
                LIMIT 3
            """)
            recent_amendment_samples = cursor.fetchall()
            print("Sample amendments with recent updates:")
            for doc_id, update_date in recent_amendment_samples:
                print(f"  source_doc_id: {doc_id}, update_date: {update_date}")

            # Check if there are actions for these amendments
            if recent_amendment_samples:
                sample_doc_ids = [row[0] for row in recent_amendment_samples]
                placeholders = ",".join(["%s"] * len(sample_doc_ids))
                cursor.execute(
                    f"""
                    SELECT source_doc_id, COUNT(*) as action_count 
                    FROM bicam_raw_congressional.amendments_actions_raw 
                    WHERE source_doc_id IN ({placeholders})
                    GROUP BY source_doc_id
                """,
                    sample_doc_ids,
                )
                action_counts = cursor.fetchall()
                print("Actions for sample amendments:")
                for doc_id, count in action_counts:
                    print(f"  {doc_id}: {count} actions")

            # Check the structure of action records
            print("\n=== Action Record Structure ===")
            cursor.execute("""
                SELECT source_doc_id, payload->>'amendments_id' as amendment_id, 
                       payload->>'text' as action_text
                FROM bicam_raw_congressional.amendments_actions_raw 
                LIMIT 3
            """)
            action_samples = cursor.fetchall()
            print("Sample action records:")
            for doc_id, amendment_id, action_text in action_samples:
                text_preview = action_text[:50] + "..." if action_text else "NULL"
                print(
                    f"  source_doc_id: {doc_id}, amendment_id: {amendment_id}, text: {text_preview}"
                )

            # Print full payload for a specific action record
            print("\n=== Full Payload for a Sample Action Record ===")
            sample_doc_id = (
                "samdt2360-119"  # You can change this to any doc_id you want to inspect
            )
            cursor.execute(
                """
                SELECT payload FROM bicam_raw_congressional.amendments_actions_raw
                WHERE source_doc_id = %s
                LIMIT 1
            """,
                (sample_doc_id,),
            )
            row = cursor.fetchone()
            if row:
                import json

                try:
                    parsed = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                    print(json.dumps(parsed, indent=2))
                except Exception:
                    print(row[0])
            else:
                print(f"No action record found for source_doc_id: {sample_doc_id}")

            # Test the extract_amendment_action_data function directly
            print("\n=== Testing extract_amendment_action_data Function ===")
            cursor.execute(
                """
                SELECT * FROM extract_amendment_action_data(%s)
            """,
                (json.dumps(parsed),),
            )
            extracted_data = cursor.fetchone()
            if extracted_data:
                print("Extracted data:")
                print(f"  amendment_id: {extracted_data[0]}")
                print(f"  action_id: {extracted_data[1]}")
                print(f"  action_text: {extracted_data[2]}")
                print(f"  action_type: {extracted_data[3]}")
                print(f"  action_code: {extracted_data[4]}")
                print(f"  action_date: {extracted_data[5]}")
                print(f"  action_time: {extracted_data[6]}")
                print(f"  source_system_code: {extracted_data[7]}")
                print(f"  source_system_name: {extracted_data[8]}")
            else:
                print("No data extracted from function")

            # Check if there are any records in the target table
            print("\n=== Checking Target Table ===")
            cursor.execute(
                "SELECT COUNT(*) FROM staging_congressional.amendments_actions"
            )
            current_count = cursor.fetchone()[0]
            print(
                f"Current records in staging_congressional.amendments_actions: {current_count}"
            )

            if current_count > 0:
                cursor.execute(
                    "SELECT * FROM staging_congressional.amendments_actions LIMIT 3"
                )
                sample_inserted = cursor.fetchall()
                print("Sample inserted records:")
                for record in sample_inserted:
                    print(f"  {record}")

            # Print a few source_doc_id values from amendments_raw
            print("\n=== Sample source_doc_id values from amendments_raw ===")
            cursor.execute("""
                SELECT source_doc_id, payload->>'updateDate' as update_date
                FROM bicam_raw_congressional.amendments_raw
                LIMIT 5
            """)
            for row in cursor.fetchall():
                print(f"  amendments_raw: source_doc_id={row[0]}, update_date={row[1]}")

            # Print a few source_doc_id values from amendments_actions_raw
            print("\n=== Sample source_doc_id values from amendments_actions_raw ===")
            cursor.execute("""
                SELECT source_doc_id, payload->>'amendments_id' as amendments_id
                FROM bicam_raw_congressional.amendments_actions_raw
                LIMIT 5
            """)
            for row in cursor.fetchall():
                print(f"  actions_raw: source_doc_id={row[0]}, amendments_id={row[1]}")

            # Show a join result
            print(
                "\n=== Sample join result (actions_raw JOIN amendments_raw ON source_doc_id) ==="
            )
            cursor.execute("""
                SELECT a.source_doc_id, a.payload->>'amendments_id', am.source_doc_id, am.payload->>'updateDate'
                FROM bicam_raw_congressional.amendments_actions_raw a
                INNER JOIN bicam_raw_congressional.amendments_raw am ON a.source_doc_id = am.source_doc_id
                WHERE (am.payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE
                LIMIT 5
            """)
            for row in cursor.fetchall():
                print(
                    f"  JOIN: a.source_doc_id={row[0]}, a.amendments_id={row[1]}, am.source_doc_id={row[2]}, am.update_date={row[3]}"
                )

            # Check for duplicates and conflicts
            print("\n=== Duplicate Analysis ===")
            cursor.execute("""
                SELECT amendment_id, action_code, action_date, COUNT(*) as count
                FROM staging_congressional.amendments_actions
                GROUP BY amendment_id, action_code, action_date
                HAVING COUNT(*) > 1
                LIMIT 5
            """)
            duplicates = cursor.fetchall()
            if duplicates:
                print(
                    "Found duplicate records (same amendment_id, action_code, action_date):"
                )
                for dup in duplicates:
                    print(f"  {dup}")
            else:
                print("No duplicate records found")

            # Check what's preventing the remaining records from being processed
            print("\n=== Remaining Records Analysis ===")
            cursor.execute("""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT a.source_doc_id
                    FROM bicam_raw_congressional.amendments_actions_raw a
                    INNER JOIN bicam_raw_congressional.amendments_raw am ON a.source_doc_id = am.source_doc_id
                    WHERE (am.payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE
                    AND a.source_doc_id NOT IN (
                        SELECT DISTINCT amendment_id FROM staging_congressional.amendments_actions
                    )
                ) as remaining
            """)
            remaining_count = cursor.fetchone()[0]
            print(f"Remaining unprocessed amendment_ids: {remaining_count}")

            # Show some examples of remaining records
            cursor.execute("""
                SELECT a.source_doc_id, a.payload->>'amendments_id', a.payload->>'text'
                FROM bicam_raw_congressional.amendments_actions_raw a
                INNER JOIN bicam_raw_congressional.amendments_raw am ON a.source_doc_id = am.source_doc_id
                WHERE (am.payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE
                AND a.source_doc_id NOT IN (
                    SELECT DISTINCT amendment_id FROM staging_congressional.amendments_actions
                )
                LIMIT 5
            """)
            remaining_examples = cursor.fetchall()
            print("Examples of remaining unprocessed records:")
            for example in remaining_examples:
                print(
                    f"  source_doc_id: {example[0]}, amendments_id: {example[1]}, text: {example[2][:50] if example[2] else 'NULL'}..."
                )

            # Check if there are any records that should be processed but aren't
            print("\n=== Processing Status Check ===")
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_actions_for_recent_amendments,
                    COUNT(DISTINCT a.source_doc_id) as unique_amendment_ids,
                    COUNT(DISTINCT CASE WHEN aa.amendment_id IS NOT NULL THEN a.source_doc_id END) as processed_amendment_ids
                FROM bicam_raw_congressional.amendments_actions_raw a
                INNER JOIN bicam_raw_congressional.amendments_raw am ON a.source_doc_id = am.source_doc_id
                LEFT JOIN staging_congressional.amendments_actions aa ON a.source_doc_id = aa.amendment_id
                WHERE (am.payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE
            """)
            status = cursor.fetchone()
            print(f"Total actions for recent amendments: {status[0]}")
            print(f"Unique amendment IDs: {status[1]}")
            print(f"Processed amendment IDs: {status[2]}")
            print(f"Unprocessed amendment IDs: {status[1] - status[2]}")

    except Exception as e:
        print(f"Error during debugging: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        processor.disconnect()


if __name__ == "__main__":
    debug_amendment_actions()
