#!/usr/bin/env python3
"""
Test script to verify imports work correctly on the server.
"""

import os
import sys

# Add the src directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, "src")
sys.path.insert(0, src_dir)

print(f"Current directory: {current_dir}")
print(f"Added to path: {src_dir}")
print(f"Python path starts with: {sys.path[:3]}")

try:
    # Test importing the constants
    from streamlined.lobbyist_matching.post_processor import (
        BILL_TYPE_VARIATIONS,
        APPROPRIATIONS_KEY_WORDS,
    )

    print("✓ Successfully imported constants from post_processor")
    print(f"House bill types: {BILL_TYPE_VARIATIONS['house']}")
    print(f"Number of key words: {len(APPROPRIATIONS_KEY_WORDS)}")

    # Test importing the matcher
    from streamlined.lobbyist_matching.matcher import ReferenceMatcher

    print("✓ Successfully imported ReferenceMatcher")

    # Test importing the batch processor
    from streamlined.lobbyist_matching.batch_processor import BatchProcessor

    print("✓ Successfully imported BatchProcessor")

    print("\n🎉 All imports successful!")

except ImportError as e:
    print(f"❌ Import failed: {e}")
    print(f"Available modules in streamlined.lobbyist_matching:")
    try:
        import streamlined.lobbyist_matching
        import os

        module_dir = os.path.dirname(streamlined.lobbyist_matching.__file__)
        files = os.listdir(module_dir)
        for f in files:
            if f.endswith(".py"):
                print(f"  - {f}")
    except Exception as e2:
        print(f"Could not list modules: {e2}")
    sys.exit(1)
