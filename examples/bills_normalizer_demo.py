#!/usr/bin/env python3
"""
Demonstration of the streamlined bills normalizer.

Shows how the normalizer handles:
1. Dictionary fields → flattened with prefixes (no clashing, e.g. actions_url vs amendments_url)
2. List fields → extracted to separate tables (cboCostEstimates, laws, notes, sponsors, etc.)
3. Nested dictionaries → recursive processing (notes/links gets its own table)
4. Uses existing source_doc_id from raw data (renamed to bill_id per config)
5. Only generates deterministic IDs for nested/related records (not main records)
"""

import uuid
from datetime import UTC, datetime
from typing import Any


class BillsNormalizerDemo:
    """Demo version of the bills normalizer logic."""

    def _flatten_dict(self, data: dict[str, Any], prefix: str = "") -> dict[str, Any]:
        """Recursively flatten a dictionary, creating prefixed column names."""
        flattened = {}

        for key, value in data.items():
            new_key = f"{prefix}_{key}" if prefix else key

            if isinstance(value, dict):
                # Recursively flatten nested dictionaries
                flattened.update(self._flatten_dict(value, new_key))
            elif isinstance(value, list):
                # Lists should be extracted separately, not flattened
                # Skip them here - they'll be handled by extract_lists
                continue
            else:
                # Store primitive values as-is
                flattened[new_key] = value

        return flattened

    def _extract_lists(
        self,
        data: dict[str, Any],
        parent_id: str,
        is_nested: bool = False,
        parent_table: str = "bills",
        root_id: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Extract list fields into separate table records (updated version)."""
        extracted = {}

        # Use root_id for nested tables, parent_id for main level
        bill_id = root_id if root_id else parent_id

        for key, value in data.items():
            if isinstance(value, list) and value:  # Non-empty lists only
                # Lowercase table names and proper nested naming
                table_name = f"{parent_table}_{key.lower()}"
                records = []

                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        # Flatten the list item and add metadata
                        record = self._flatten_dict(item)
                        record["bill_id"] = bill_id  # Always reference the main bill
                        record["list_index"] = i
                        record["extracted_at"] = datetime.now(UTC).isoformat()

                        if is_nested:
                            # For nested records, id should reference the immediate parent
                            record["id"] = parent_id
                        else:
                            # For main level records, generate new deterministic ID
                            record_key = f"{parent_id}_{key}_{i}"
                            record["id"] = str(
                                uuid.uuid5(uuid.NAMESPACE_DNS, record_key)
                            )

                        # Handle nested lists within the item with proper table naming
                        nested_lists = self._extract_lists(
                            item,
                            record["id"],
                            is_nested=True,
                            parent_table=table_name,
                            root_id=bill_id,
                        )
                        for nested_table, nested_records in nested_lists.items():
                            if nested_table not in extracted:
                                extracted[nested_table] = []
                            extracted[nested_table].extend(nested_records)

                        records.append(record)
                    else:
                        # Handle primitive list items
                        record = {
                            "bill_id": bill_id,  # Always reference the main bill
                            "list_index": i,
                            "value": str(item),
                            "extracted_at": datetime.now(UTC).isoformat(),
                        }

                        if is_nested:
                            # For nested records, id should reference the immediate parent
                            record["id"] = parent_id
                        else:
                            # For main level records, generate new deterministic ID
                            record_key = f"{parent_id}_{key}_{i}"
                            record["id"] = str(
                                uuid.uuid5(uuid.NAMESPACE_DNS, record_key)
                            )

                        records.append(record)

                if records:
                    extracted[table_name] = records

        return extracted

    def process_bill_record(
        self, raw_row: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        """Process a single bill record: flatten dicts and extract lists."""
        # Extract the bill data and existing ID (simulated source_doc_id)
        bill_data = raw_row.get("data", {}).get("bill", raw_row)  # Fallback for demo
        existing_id = raw_row.get("source_doc_id", "hr3076-117")  # Simulated ID

        # Flatten all dictionary fields
        flattened = self._flatten_dict(bill_data)

        # Use existing ID as bill_id (from config id_fields)
        flattened["bill_id"] = existing_id
        flattened["processed_at"] = datetime.now(UTC).isoformat()

        # Extract all list fields
        extracted_lists = self._extract_lists(
            bill_data, existing_id, is_nested=False, parent_table="bills", root_id=None
        )

        return flattened, extracted_lists

    def process_related_data(
        self,
        related_data: list[dict[str, Any]],
        bill_id: str,
        data_type: str = "actions",
    ) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
        """Process related data (like actions) for a bill."""
        flattened_records = []
        all_extracted_data = {}

        for i, item_data in enumerate(related_data):
            # Flatten the related item
            flattened = self._flatten_dict(item_data)

            # Add metadata
            flattened["bill_id"] = bill_id
            flattened["list_index"] = i
            flattened["processed_at"] = datetime.now(UTC).isoformat()

            # Generate deterministic ID for this related record
            record_key = f"{bill_id}_{data_type}_{i}"
            record_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, record_key))
            flattened["id"] = record_id

            flattened_records.append(flattened)

            # Extract any nested lists from this related record
            # Set is_nested=True so nested records use the action's ID as their parent reference
            extracted_lists = self._extract_lists(
                item_data,
                record_id,
                is_nested=True,
                parent_table=f"bills_{data_type}",
                root_id=bill_id,
            )

            # Merge extracted data
            for table_name, records in extracted_lists.items():
                if table_name not in all_extracted_data:
                    all_extracted_data[table_name] = []
                all_extracted_data[table_name].extend(records)

        return flattened_records, all_extracted_data


def demo():
    """Demonstrate the normalization process with example data."""

    # Example related actions data from bills_actions_raw
    example_actions = [
        {
            "actionCode": "8000",
            "actionDate": "2001-06-05",
            "actionTime": "18:25:23",
            "recordedVotes": [
                {
                    "chamber": "House",
                    "congress": 107,
                    "date": "2001-06-05T22:25:23Z",
                    "rollNumber": 150,
                    "sessionNumber": 1,
                    "url": "https://clerk.house.gov/evs/2001/roll150.xml",
                }
            ],
            "sourceSystem": {"code": 9, "name": "Library of Congress"},
            "text": "Passed/agreed to in House: On motion to suspend the rules and agree to the resolution, as amended Agreed to by the Yeas and Nays: (2/3 required): 405 - 0 (Roll no. 150).(text: CR H2853)",
            "type": "Floor",
        },
        {
            "actionCode": "H11100",
            "actionDate": "2001-04-04",
            "committees": [
                {
                    "name": "Education and Workforce Committee",
                    "systemCode": "hsed00",
                    "url": "https://api.congress.gov/v3/committee/house/hsed00?format=json",
                }
            ],
            "sourceSystem": {"code": 2, "name": "House floor actions"},
            "text": "Referred to the House Committee on Education and the Workforce.",
            "type": "IntroReferral",
        },
        {
            "actionCode": "H30300",
            "actionDate": "2001-06-05",
            "actionTime": "14:15:42",
            "sourceSystem": {"code": 2, "name": "House floor actions"},
            "text": "Mr. Osborne moved to suspend the rules and agree to the resolution, as amended.",
            "type": "Floor",
        },
    ]

    # Example bill data from the user
    example_bill = {
        "actions": {
            "count": 74,
            "url": "https://api.congress.gov/v3/bill/117/hr/3076/actions?format=json",
        },
        "amendments": {
            "count": 48,
            "url": "https://api.congress.gov/v3/bill/117/hr/3076/amendments?format=json",
        },
        "cboCostEstimates": [
            {
                "description": "As ordered reported by the House Committee on Oversight and Reform on May 13, 2021\n",
                "pubDate": "2021-07-14T17:27:00Z",
                "title": "H.R. 3076, Postal Service Reform Act of 2021",
                "url": "https://www.cbo.gov/publication/57356",
            },
            {
                "description": "As Posted on February 3, 2022,\nand as Amended by Amendment #1, the Manager's Amendment, as Posted on February 4, 2022\n",
                "pubDate": "2022-02-04T18:03:00Z",
                "title": "Estimated Budgetary Effects of Rules Committee Print 117-32 for H.R. 3076, the Postal Service Reform Act of 2022",
                "url": "https://www.cbo.gov/publication/57821",
            },
        ],
        "committeeReports": [
            {
                "citation": "H. Rept. 117-89,Part 1",
                "url": "https://api.congress.gov/v3/committee-report/117/HRPT/89?format=json",
            }
        ],
        "committees": {
            "count": 3,
            "url": "https://api.congress.gov/v3/bill/117/hr/3076/committees?format=json",
        },
        "congress": 117,
        "cosponsors": {
            "count": 102,
            "countIncludingWithdrawnCosponsors": 102,
            "url": "https://api.congress.gov/v3/bill/117/hr/3076/cosponsors?format=json",
        },
        "introducedDate": "2021-05-11",
        "latestAction": {
            "actionDate": "2022-04-06",
            "text": "Became Public Law No: 117-108.",
        },
        "laws": [{"number": "117-108", "type": "Public Law"}],
        "number": "3076",
        "notes": [
            {
                "links": [
                    {
                        "name": "H.R. 1579",
                        "url": "https://www.congress.gov/bill/108th-congress/house-bill/1579",
                    },
                    {
                        "name": "H.R. 2443",
                        "url": "https://www.congress.gov/bill/108th-congress/house-bill/2443",
                    },
                ],
                "text": "Language similar to H.R. 1579 was incorporated in section 604 of H.R. 2443, the Coast Guard and Maritime Transportation Act of 2003, as reported in House.",
            }
        ],
        "originChamber": "House",
        "policyArea": {"name": "Government Operations and Politics"},
        "sponsors": [
            {
                "bioguideId": "M000087",
                "district": 12,
                "firstName": "CAROLYN",
                "fullName": "Rep. Maloney, Carolyn B. [D-NY-12]",
                "isByRequest": "N",
                "lastName": "MALONEY",
                "middleName": "B.",
                "party": "D",
                "state": "NY",
                "url": "https://api.congress.gov/v3/member/M000087?format=json",
            }
        ],
        "subjects": {
            "count": 17,
            "url": "https://api.congress.gov/v3/bill/117/hr/3076/subjects?format=json",
        },
        "title": "Postal Service Reform Act of 2022",
        "type": "HR",
        "updateDate": "2022-09-29T03:27:05Z",
        "updateDateIncludingText": "2022-09-29T03:27:05Z",
    }

    # Simulate raw database row format for bill
    raw_row = {
        "data": {"bill": example_bill},
        "source_doc_id": "hr3076-117",  # This comes from extract_item_id in the fetcher
    }

    # Process the bill and actions
    normalizer = BillsNormalizerDemo()

    # Process main bill data
    bill_flattened, bill_extracted = normalizer.process_bill_record(raw_row)

    # Process related actions data (simulating data from bills_actions_raw)
    actions_flattened, actions_extracted = normalizer.process_related_data(
        example_actions, "s2092-119", "actions"
    )

    print("=== BILL DATA: FLATTENED MAIN TABLE ===")
    print("Dictionary fields flattened with prefixes:")
    for key, value in sorted(bill_flattened.items()):
        if key.startswith(
            (
                "actions_",
                "amendments_",
                "committees_",
                "cosponsors_",
                "latestAction_",
                "policyArea_",
                "subjects_",
            )
        ):
            print(f"  {key}: {value}")

    print("\nOther main fields:")
    for key, value in sorted(bill_flattened.items()):
        if not key.startswith(
            (
                "actions_",
                "amendments_",
                "committees_",
                "cosponsors_",
                "latestAction_",
                "policyArea_",
                "subjects_",
            )
        ):
            print(f"  {key}: {value}")

    print("\n=== BILL DATA: EXTRACTED TABLES FROM NESTED LISTS ===")
    print("List fields extracted to separate tables:")

    for table_name, records in bill_extracted.items():
        print(f"\nTable: {table_name} ({len(records)} records)")
        for i, record in enumerate(records):
            print(f"  Record {i + 1}:")
            for key, value in sorted(record.items()):
                if key in ["bill_id", "list_index", "extracted_at", "id"]:
                    print(f"    {key}: {value}")
                else:
                    print(f"    {key}: {value}")

    print("\n" + "=" * 60)
    print("=== ACTIONS DATA: RELATED TABLE (bills_actions) ===")
    print("Actions with flattened sourceSystem fields:")

    for i, action in enumerate(actions_flattened):
        print(f"\nAction {i + 1}:")
        for key, value in sorted(action.items()):
            if key in ["bill_id", "list_index", "extracted_at", "id"]:
                print(f"  {key}: {value}")
            elif key.startswith("sourceSystem_"):
                print(f"  {key}: {value}  # Flattened from nested object")
            else:
                print(f"  {key}: {value}")

    print("\n=== ACTIONS DATA: EXTRACTED TABLES FROM NESTED LISTS ===")
    print("Nested lists within actions extracted to separate tables:")

    for table_name, records in actions_extracted.items():
        print(f"\nTable: {table_name} ({len(records)} records)")
        for i, record in enumerate(records):
            print(f"  Record {i + 1}:")
            for key, value in sorted(record.items()):
                if key in ["bill_id", "list_index", "extracted_at", "id"]:
                    print(f"    {key}: {value}")
                else:
                    print(f"    {key}: {value}")

    print("\n" + "=" * 60)
    print("=== COMPREHENSIVE SUMMARY ===")
    all_bill_records = sum(len(records) for records in bill_extracted.values())
    all_actions_records = len(actions_flattened) + sum(
        len(records) for records in actions_extracted.values()
    )

    print(f"BILL DATA:")
    print(f"  Main table (bills) columns: {len(bill_flattened)}")
    print(f"  Extracted tables from lists: {len(bill_extracted)}")
    print(f"  Total bill extracted records: {all_bill_records}")

    print(f"\nACTIONS DATA:")
    print(f"  Main related table (bills_actions) records: {len(actions_flattened)}")
    print(f"  Extracted tables from nested lists: {len(actions_extracted)}")
    print(
        f"  Total actions extracted records: {sum(len(records) for records in actions_extracted.values())}"
    )

    print(f"\nOVERALL:")
    print(
        f"  Total tables created: {1 + len(bill_extracted) + 1 + len(actions_extracted)}"
    )
    print(f"  Total records processed: {1 + all_bill_records + all_actions_records}")

    print(f"\nComplete table breakdown:")
    print(f"  bills: 1 record (main)")
    for table_name, records in bill_extracted.items():
        print(f"  {table_name}: {len(records)} records")
    print(f"  bills_actions: {len(actions_flattened)} records (related)")
    for table_name, records in actions_extracted.items():
        print(f"  {table_name}: {len(records)} records")


if __name__ == "__main__":
    demo()
