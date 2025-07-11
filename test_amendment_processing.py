#!/usr/bin/env python3
"""
Test script for amendment processing

This script demonstrates how to use the amendment processing functions
with the provided JSON examples.
"""

import json
from process_amendments import AmendmentProcessor

# Example JSON payloads from the user
EXAMPLE_AMENDMENTS = [
    {
        "type": "SAMDT",
        "number": "584",
        "actions": {
            "url": "https://api.congress.gov/v3/amendment/112/samdt/584/actions?format=json",
            "count": 3,
        },
        "chamber": "Senate",
        "purpose": "Of a perfecting nature.",
        "congress": 112,
        "sponsors": [
            {
                "url": "https://api.congress.gov/v3/member/R000146?format=json",
                "party": "D",
                "state": "NV",
                "fullName": "Sen. Reid, Harry [D-NV]",
                "lastName": "REID",
                "firstName": "HARRY",
                "bioguideId": "R000146",
                "middleName": "M.",
            }
        ],
        "updateDate": "2025-07-02T21:32:29Z",
        "amendedBill": {
            "url": "https://api.congress.gov/v3/bill/112/s/1323?format=json",
            "type": "S",
            "title": "A bill to express the sense of the Senate on shared sacrifice in resolving the budget deficit.",
            "number": "1323",
            "congress": 112,
            "originChamber": "Senate",
            "originChamberCode": "S",
            "updateDateIncludingText": "2025-07-02",
        },
        "latestAction": {
            "text": "Amendment SA 584 proposed by Senator Reid to Amendment SA 583, the instructions of the motion to commit. (consideration: CR S4866; text: CR S4866) Of a perfecting nature.",
            "links": [
                {
                    "url": "https://www.congress.gov/congressional-record/volume-157/senate-section/page/S4866",
                    "name": "S4866",
                },
                {
                    "url": "https://www.congress.gov/amendment/112th-congress/senate-amendment/583",
                    "name": "SA 583",
                },
                {
                    "url": "https://www.congress.gov/amendment/112th-congress/senate-amendment/584",
                    "name": "SA 584",
                },
            ],
            "actionDate": "2011-07-25",
        },
        "proposedDate": "2011-07-25T04:00:00Z",
        "textVersions": {
            "url": "https://api.congress.gov/v3/amendment/112/samdt/584/text?format=json",
            "count": 2,
        },
        "submittedDate": "2011-07-25T04:00:00Z",
        "amendedAmendment": {
            "url": "https://api.congress.gov/v3/amendment/112/samdt/583?format=json",
            "type": "SAMDT",
            "number": "583",
            "purpose": "To change the enactment date.",
            "congress": 112,
            "updateDate": "2025-07-02T21:32:29Z",
        },
        "amendmentsToAmendment": {
            "url": "https://api.congress.gov/v3/amendment/112/samdt/584/amendments?format=json",
            "count": 1,
        },
    },
    {
        "type": "SAMDT",
        "number": "96",
        "actions": {
            "url": "https://api.congress.gov/v3/amendment/116/samdt/96/actions?format=json",
            "count": 6,
        },
        "chamber": "Senate",
        "purpose": "To clarify that the amendment shall not be construed as a declaration of war or an authorization of the use of military force.",
        "congress": 116,
        "sponsors": [
            {
                "url": "https://api.congress.gov/v3/member/M000639?format=json",
                "party": "D",
                "state": "NJ",
                "fullName": "Sen. Menendez, Robert [D-NJ]",
                "lastName": "Menendez",
                "firstName": "Robert",
                "bioguideId": "M000639",
            }
        ],
        "cosponsors": {
            "url": "https://api.congress.gov/v3/amendment/116/samdt/96/cosponsors?format=json",
            "count": 1,
            "countIncludingWithdrawnCosponsors": 1,
        },
        "updateDate": "2022-02-08T23:22:08Z",
        "amendedBill": {
            "url": "https://api.congress.gov/v3/bill/116/s/1?format=json",
            "type": "S",
            "title": "Strengthening America's Security in the Middle East Act of 2019",
            "number": "1",
            "congress": 116,
            "originChamber": "Senate",
            "originChamberCode": "S",
            "updateDateIncludingText": "2025-05-28",
        },
        "latestAction": {
            "text": "Amendment SA 96 agreed to in Senate by Voice Vote. ",
            "links": [
                {
                    "url": "https://www.congress.gov/amendment/116th-congress/senate-amendment/96",
                    "name": "SA 96",
                }
            ],
            "actionDate": "2019-02-04",
        },
        "proposedDate": "2019-01-31T05:00:00Z",
        "textVersions": {
            "url": "https://api.congress.gov/v3/amendment/116/samdt/96/text?format=json",
            "count": 2,
        },
        "submittedDate": "2019-01-31T05:00:00Z",
        "amendedAmendment": {
            "url": "https://api.congress.gov/v3/amendment/116/samdt/65?format=json",
            "type": "SAMDT",
            "number": "65",
            "purpose": "To express the sense of the Senate that the United States faces continuing threats from terrorist groups operating in Syria and Afghanistan and that the precipitous withdrawal of United States forces from either country could put at risk hard-won gains and United States national security.",
            "congress": 116,
            "updateDate": "2022-02-08T23:22:07Z",
        },
    },
    {
        "type": "SAMDT",
        "number": "1445",
        "actions": {
            "url": "https://api.congress.gov/v3/amendment/117/samdt/1445/actions?format=json",
            "count": 9,
        },
        "chamber": "Senate",
        "purpose": "To improve the bill.",
        "congress": 117,
        "sponsors": [
            {
                "url": "https://api.congress.gov/v3/member/H001042?format=json",
                "party": "D",
                "state": "HI",
                "fullName": "Sen. Hirono, Mazie K. [D-HI]",
                "lastName": "Hirono",
                "firstName": "Mazie",
                "bioguideId": "H001042",
                "middleName": "K.",
            }
        ],
        "cosponsors": {
            "url": "https://api.congress.gov/v3/amendment/117/samdt/1445/cosponsors?format=json",
            "count": 3,
            "countIncludingWithdrawnCosponsors": 3,
        },
        "updateDate": "2022-02-09T12:39:28Z",
        "amendedBill": {
            "url": "https://api.congress.gov/v3/bill/117/s/937?format=json",
            "type": "S",
            "title": "COVID-19 Hate Crimes Act",
            "number": "937",
            "congress": 117,
            "originChamber": "Senate",
            "originChamberCode": "S",
            "updateDateIncludingText": "2025-05-28",
        },
        "latestAction": {
            "text": "Amendment SA 1445 agreed to in Senate by Unanimous Consent. ",
            "links": [
                {
                    "url": "https://www.congress.gov/amendment/117th-congress/senate-amendment/1445",
                    "name": "SA 1445",
                }
            ],
            "actionDate": "2021-04-22",
        },
        "proposedDate": "2021-04-19T04:00:00Z",
        "textVersions": {
            "url": "https://api.congress.gov/v3/amendment/117/samdt/1445/text?format=json",
            "count": 1,
        },
        "submittedDate": "2021-04-19T04:00:00Z",
        "amendmentsToAmendment": {
            "url": "https://api.congress.gov/v3/amendment/117/samdt/1445/amendments?format=json",
            "count": 7,
        },
    },
]


def test_amendment_processing():
    """Test the amendment processing with example data"""

    # Database configuration - update these values for your environment
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "your_database_name",
        "user": "your_username",
        "password": "your_password",
    }

    processor = AmendmentProcessor(db_config)

    try:
        # Connect to database
        processor.connect()

        # Load SQL functions
        processor.load_sql_functions()

        # Convert examples to JSON strings
        json_strings = [json.dumps(amendment) for amendment in EXAMPLE_AMENDMENTS]

        # Process the examples
        processor.process_json_strings(json_strings, batch_size=10)

        # Get processing statistics
        stats = processor.get_processing_stats()
        print("Processing Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        processor.disconnect()


def test_raw_table_processing():
    """Test processing from the raw table"""

    # Database configuration - update these values for your environment
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "your_database_name",
        "user": "your_username",
        "password": "your_password",
    }

    processor = AmendmentProcessor(db_config)

    try:
        # Connect to database
        processor.connect()

        # Load SQL functions
        processor.load_sql_functions()

        # Process a small batch from the raw table for testing
        result = processor.process_from_raw_table(
            batch_size=100, start_offset=0, max_records=500
        )

        print("Raw Table Processing Results:")
        for key, value in result.items():
            print(f"  {key}: {value}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        processor.disconnect()


if __name__ == "__main__":
    print("Testing amendment processing with example data...")
    test_amendment_processing()

    print("\nTesting raw table processing...")
    test_raw_table_processing()
