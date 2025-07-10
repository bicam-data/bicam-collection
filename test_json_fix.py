#!/usr/bin/env python3
"""
Test script to verify JSON parsing fixes work correctly.
"""

import json
import re


def test_json_parsing_fix():
    """Test the JSON parsing fixes we implemented."""

    # Test case 1: Double-escaped quotes (the original issue)
    problematic_json = '[{"parsed": "JIM WEBB of Virginia", "authority-fnf": "Jim Webb", "authority-lnf": "Webb, Jim", "authority-other": "Mr. James H. \\"Jim\\" Webb"}]'

    print("Test 1: Double-escaped quotes")
    print(f"Original: {problematic_json}")

    # Apply our fix
    cleaned_data = problematic_json

    # Handle double-escaped quotes (common issue)
    if '\\"' in cleaned_data:
        cleaned_data = cleaned_data.replace('\\"', '"')

    # Handle other common JSON issues
    if cleaned_data.startswith('"') and cleaned_data.endswith('"'):
        cleaned_data = cleaned_data[1:-1]

    try:
        items = json.loads(cleaned_data)
        print(f"✅ Successfully parsed: {items}")
    except json.JSONDecodeError as e:
        print(f"❌ Still failed: {e}")

        # Try regex fallback
        try:
            json_match = re.search(r"\[.*\]", cleaned_data)
            if json_match:
                potential_json = json_match.group(0)
                potential_json = potential_json.replace('\\"', '"')
                potential_json = re.sub(r",(\s*[}\]])", r"\1", potential_json)
                items = json.loads(potential_json)
                print(f"✅ Successfully parsed with regex fallback: {items}")
            else:
                print("❌ Regex fallback failed")
        except Exception as e2:
            print(f"❌ Regex fallback also failed: {e2}")

    print()

    # Test case 2: Trailing commas
    problematic_json2 = '[{"name": "John", "age": 30,}, {"name": "Jane", "age": 25,}]'

    print("Test 2: Trailing commas")
    print(f"Original: {problematic_json2}")

    cleaned_data2 = problematic_json2
    cleaned_data2 = re.sub(r",(\s*[}\]])", r"\1", cleaned_data2)

    try:
        items = json.loads(cleaned_data2)
        print(f"✅ Successfully parsed: {items}")
    except json.JSONDecodeError as e:
        print(f"❌ Failed: {e}")

    print()

    # Test case 3: Wrapped in quotes
    problematic_json3 = '"[{\\"name\\": \\"John\\", \\"age\\": 30}]"'

    print("Test 3: Wrapped in quotes with escaped quotes")
    print(f"Original: {problematic_json3}")

    cleaned_data3 = problematic_json3

    # Handle double-escaped quotes
    if '\\"' in cleaned_data3:
        cleaned_data3 = cleaned_data3.replace('\\"', '"')

    # Remove outer quotes if present
    if cleaned_data3.startswith('"') and cleaned_data3.endswith('"'):
        cleaned_data3 = cleaned_data3[1:-1]

    try:
        items = json.loads(cleaned_data3)
        print(f"✅ Successfully parsed: {items}")
    except json.JSONDecodeError as e:
        print(f"❌ Failed: {e}")

    print()
    print("All tests completed!")


if __name__ == "__main__":
    test_json_parsing_fix()
