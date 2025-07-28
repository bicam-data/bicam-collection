#!/usr/bin/env python3
"""
Test script to verify all imports work correctly in the lobbyist_matching module.
"""

import sys


def test_imports():
    """Test that all modules can be imported correctly."""
    print("Testing imports...")

    try:
        # Test main module imports
        from .main import match_only, post_process_all, process_filings

        print("✅ main.py imports work")
    except ImportError as e:
        print(f"❌ main.py imports failed: {e}")
        return False

    try:
        # Test batch processor imports
        from .batch_processor import BatchProcessor

        print("✅ batch_processor.py imports work")
    except ImportError as e:
        print(f"❌ batch_processor.py imports failed: {e}")
        return False

    try:
        # Test matcher imports
        from .matcher import MatchingManager, ReferenceMatcher

        print("✅ matcher.py imports work")
    except ImportError as e:
        print(f"❌ matcher.py imports failed: {e}")
        return False

    try:
        # Test post processor imports
        from .post_processor import post_process_all

        print("✅ post_processor.py imports work")
    except ImportError as e:
        print(f"❌ post_processor.py imports failed: {e}")
        return False

    try:
        # Test section processor imports
        from .section_processor import process_single_section

        print("✅ section_processor.py imports work")
    except ImportError as e:
        print(f"❌ section_processor.py imports failed: {e}")
        return False

    try:
        # Test db_utils imports
        from .db_utils import DatabaseInterface, FilingSection

        print("✅ db_utils.py imports work")
    except ImportError as e:
        print(f"❌ db_utils.py imports failed: {e}")
        return False

    print("\n🎉 All imports successful!")
    return True


if __name__ == "__main__":
    success = test_imports()
    sys.exit(0 if success else 1)
