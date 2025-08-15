#!/usr/bin/env python3
"""
Example script showing how to use the new timeout reprocessing CLI flag.

This demonstrates the different ways to reprocess timeout sections.
"""

import subprocess
import sys


def run_timeout_reprocessing_examples():
    """Show examples of how to use the timeout reprocessing feature."""

    print("=== Timeout Reprocessing Examples ===\n")

    # Example 1: Just reprocess timeouts (no matching/post-processing)
    print("1. Reprocess timeouts only (no matching/post-processing):")
    print(
        "python src/streamlined/lobbyist_matching/run_pre2008_lda.py --reprocess-timeouts-only 123"
    )
    print("   - This will reprocess all timeout sections for run_id 123")
    print("   - Skips the initial processing phase")
    print("   - Does NOT run matching or post-processing")
    print()

    # Example 2: Reprocess timeouts with matching and post-processing
    print("2. Reprocess timeouts with matching and post-processing:")
    print(
        "python src/streamlined/lobbyist_matching/run_pre2008_lda.py --reprocess-timeouts-only 123 --run-matching-after-timeouts"
    )
    print("   - This will reprocess all timeout sections for run_id 123")
    print("   - Then runs matching on ALL references (original + new from timeouts)")
    print("   - Then runs post-processing on ALL matches")
    print()

    # Example 3: Custom chunk size for timeout reprocessing
    print("3. Reprocess timeouts with custom chunk size:")
    print(
        "python src/streamlined/lobbyist_matching/run_pre2008_lda.py --reprocess-timeouts-only 123 --timeout-chunk-size 10 --run-matching-after-timeouts"
    )
    print("   - Uses 10 chunks per section instead of default 5")
    print("   - Useful for very long sections that still timeout")
    print()

    # Example 4: Check timeout status first
    print("4. Check timeout status before reprocessing:")
    print("python check_timeout_status.py 123")
    print("   - Shows how many timeouts occurred")
    print("   - Shows whether any were already reprocessed")
    print("   - Shows overall run statistics")
    print()

    print("=== When to Use Each Option ===")
    print()
    print("Use --reprocess-timeouts-only when:")
    print("  - You want to add more references from timeout sections")
    print("  - You don't want to re-run matching on existing references")
    print("  - You want to manually control when matching happens")
    print()
    print("Use --reprocess-timeouts-only --run-matching-after-timeouts when:")
    print("  - You want to reprocess timeouts AND update all matches")
    print("  - You want the complete pipeline to run on new data")
    print("  - You want to ensure all references are matched")
    print()
    print("Use --timeout-chunk-size when:")
    print("  - Sections are still timing out during reprocessing")
    print("  - You want smaller chunks for better success rate")
    print("  - You're dealing with very long text sections")


if __name__ == "__main__":
    run_timeout_reprocessing_examples()
