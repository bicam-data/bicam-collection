#!/usr/bin/env python3
"""
Test script to verify Python 3.10 compatibility.
"""

import sys
import os

print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")

# Add the src directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, "src")
sys.path.insert(0, src_dir)

print(f"Added to path: {src_dir}")

try:
    # Test importing the streamlined module
    import streamlined
    print("✓ Successfully imported streamlined module")
    
    # Test importing the lobbyist matching module
    import streamlined.lobbyist_matching
    print("✓ Successfully imported lobbyist_matching module")
    
    # Test importing specific components
    from streamlined.lobbyist_matching.post_processor import BILL_TYPE_VARIATIONS, APPROPRIATIONS_KEY_WORDS
    print("✓ Successfully imported post_processor constants")
    
    from streamlined.lobbyist_matching.matcher import ReferenceMatcher
    print("✓ Successfully imported ReferenceMatcher")
    
    from streamlined.lobbyist_matching.batch_processor import BatchProcessor
    print("✓ Successfully imported BatchProcessor")
    
    # Test importing API clients (which had the UTC issue)
    from streamlined.api_clients.congressional_api import CongressionalAPIClient
    print("✓ Successfully imported CongressionalAPIClient")
    
    from streamlined.api_clients.base_api_client import BaseAPIClient
    print("✓ Successfully imported BaseAPIClient")
    
    print("\n🎉 All imports successful! Python 3.10 compatibility confirmed.")
    
except ImportError as e:
    print(f"❌ Import failed: {e}")
    print(f"Error type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
