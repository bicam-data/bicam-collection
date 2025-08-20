#!/usr/bin/env python3
"""
Script to analyze matching logs and provide insights into matching performance.

This script reads the JSONL log files created by the MatchingLogger and provides
statistics on which matching methods worked and which failed.
"""

import argparse
import json
import os
from collections import defaultdict, Counter
from typing import Dict, List, Any


def load_log_file(log_file_path: str) -> List[Dict[str, Any]]:
    """Load and parse a JSONL log file.

    Args:
        log_file_path (str): Path to the JSONL log file

    Returns:
        List[Dict[str, Any]]: List of log entries
    """
    entries = []

    if not os.path.exists(log_file_path):
        print(f"Log file not found: {log_file_path}")
        return entries

    with open(log_file_path, "r") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                entry = json.loads(line)
                entries.append(entry)
            except json.JSONDecodeError as e:
                print(f"Error parsing line {line_num}: {e}")
                continue

    return entries


def analyze_matching_performance(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze matching performance from log entries.

    Args:
        entries (List[Dict[str, Any]]): List of log entries

    Returns:
        Dict[str, Any]: Analysis results
    """
    # Separate matching attempts from final results
    attempts = [e for e in entries if e.get("event_type") != "final_result"]
    final_results = [e for e in entries if e.get("event_type") == "final_result"]

    # Analyze matching attempts
    method_stats = defaultdict(lambda: {"success": 0, "failure": 0, "total": 0})
    reference_stats = defaultdict(
        lambda: {"methods_attempted": [], "final_result": None}
    )

    for attempt in attempts:
        method = attempt.get("method_attempted", "unknown")
        success = attempt.get("success", False)

        method_stats[method]["total"] += 1
        if success:
            method_stats[method]["success"] += 1
        else:
            method_stats[method]["failure"] += 1

    # Analyze final results
    final_result_counts = Counter()
    confidence_ranges = defaultdict(int)

    for result in final_results:
        match_type = result.get("final_match_type", "unknown")
        confidence = result.get("final_confidence", 0.0)

        final_result_counts[match_type] += 1

        # Categorize confidence scores
        if confidence == 0.0:
            confidence_ranges["0.0 (unmatched)"] += 1
        elif confidence < 0.3:
            confidence_ranges["0.0-0.3 (low)"] += 1
        elif confidence < 0.7:
            confidence_ranges["0.3-0.7 (medium)"] += 1
        elif confidence < 1.0:
            confidence_ranges["0.7-1.0 (high)"] += 1
        else:
            confidence_ranges["1.0 (exact)"] += 1

    return {
        "total_attempts": len(attempts),
        "total_final_results": len(final_results),
        "method_stats": dict(method_stats),
        "final_result_counts": dict(final_result_counts),
        "confidence_ranges": dict(confidence_ranges),
        "success_rate_by_method": {
            method: {
                "success_rate": stats["success"] / stats["total"]
                if stats["total"] > 0
                else 0,
                "total_attempts": stats["total"],
                "successes": stats["success"],
                "failures": stats["failure"],
            }
            for method, stats in method_stats.items()
        },
    }


def print_analysis(analysis: Dict[str, Any], log_file_path: str):
    """Print formatted analysis results.

    Args:
        analysis (Dict[str, Any]): Analysis results
        log_file_path (str): Path to the log file
    """
    print(f"\n{'=' * 60}")
    print(f"MATCHING PERFORMANCE ANALYSIS")
    print(f"Log file: {log_file_path}")
    print(f"{'=' * 60}")

    print(f"\n📊 OVERVIEW:")
    print(f"  Total matching attempts: {analysis['total_attempts']:,}")
    print(f"  Total final results: {analysis['total_final_results']:,}")

    print(f"\n🎯 FINAL RESULTS:")
    for result_type, count in analysis["final_result_counts"].items():
        percentage = (
            (count / analysis["total_final_results"]) * 100
            if analysis["total_final_results"] > 0
            else 0
        )
        print(f"  {result_type}: {count:,} ({percentage:.1f}%)")

    print(f"\n📈 CONFIDENCE SCORE DISTRIBUTION:")
    for range_name, count in analysis["confidence_ranges"].items():
        percentage = (
            (count / analysis["total_final_results"]) * 100
            if analysis["total_final_results"] > 0
            else 0
        )
        print(f"  {range_name}: {count:,} ({percentage:.1f}%)")

    print(f"\n🔍 METHOD PERFORMANCE:")
    for method, stats in analysis["success_rate_by_method"].items():
        success_rate = stats["success_rate"] * 100
        print(f"  {method}:")
        print(f"    Success rate: {success_rate:.1f}%")
        print(f"    Total attempts: {stats['total_attempts']:,}")
        print(f"    Successes: {stats['successes']:,}")
        print(f"    Failures: {stats['failures']:,}")

    # Identify most successful and problematic methods
    print(f"\n🏆 TOP PERFORMING METHODS:")
    sorted_methods = sorted(
        analysis["success_rate_by_method"].items(),
        key=lambda x: x[1]["success_rate"],
        reverse=True,
    )

    for i, (method, stats) in enumerate(sorted_methods[:5], 1):
        if stats["total_attempts"] > 0:
            print(f"  {i}. {method}: {stats['success_rate'] * 100:.1f}% success rate")

    print(f"\n⚠️  METHODS WITH LOW SUCCESS RATES:")
    for method, stats in sorted_methods:
        if stats["total_attempts"] > 10 and stats["success_rate"] < 0.5:
            print(
                f"  {method}: {stats['success_rate'] * 100:.1f}% success rate ({stats['total_attempts']} attempts)"
            )


def main():
    """Main function to run the analysis."""
    parser = argparse.ArgumentParser(description="Analyze matching performance logs")
    parser.add_argument("log_file", help="Path to the JSONL log file to analyze")
    parser.add_argument(
        "--output", help="Output file for detailed results (JSON format)"
    )

    args = parser.parse_args()

    # Load and analyze the log file
    entries = load_log_file(args.log_file)

    if not entries:
        print("No valid log entries found.")
        return

    analysis = analyze_matching_performance(entries)
    print_analysis(analysis, args.log_file)

    # Save detailed results if requested
    if args.output:
        with open(args.output, "w") as f:
            json.dump(analysis, f, indent=2)
        print(f"\n📄 Detailed results saved to: {args.output}")


if __name__ == "__main__":
    main()
