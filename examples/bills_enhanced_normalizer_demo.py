#!/usr/bin/env python3
"""
Enhanced Bills Database Normalizer Demo

This demo shows the improved Bills Database Normalizer that:
1. Uses proper checkpointing and progress tracking
2. Processes data in batches for better memory management
3. Dynamically discovers related tables from configuration
4. Handles both bills and all related raw data
5. Provides comprehensive run tracking
"""

import asyncio
import logging
from pathlib import Path

# Setup basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


# Simulated database normalizer for demonstration
class EnhancedBillsNormalizerDemo:
    """Demo version of the enhanced Bills Database Normalizer."""

    def __init__(self, config_path: str | None = None):
        self.config_path = config_path or str(
            Path(__file__).parent.parent
            / "src"
            / "bicam_collection"
            / "data_types"
            / "congressional"
            / "bills"
            / "config.yaml"
        )

    def _get_related_tables_with_raw_data(self) -> list[str]:
        """Get list of related table suffixes that have raw data tables from config."""
        # Simulated loading from config.yaml
        # In reality this would use get_all_data_type_configs()

        print(f"📋 Loading configuration from: {self.config_path}")

        # These are the tables with create_raw: true from the config
        config_based_tables = [
            "actions",  # bills_actions (create_raw: true)
            "cosponsors",  # bills_cosponsors (create_raw: true)
            "subjects",  # bills_subjects (create_raw: true)
            "summaries",  # bills_summaries (create_raw: true)
            "texts",  # bills_texts (create_raw: true)
            "titles",  # bills_titles (create_raw: true)
            "relatedbills",  # bills_relatedbills (create_raw: true)
        ]

        # These tables exist in config but have create_raw: false, so excluded:
        # - bills_actions_committees (create_raw: false)
        # - bills_actions_recorded_votes (create_raw: false)
        # - bills_committeeactivities (no create_raw field, defaults to false)
        # - bills_cbocostestimates (create_raw: false)
        # - bills_laws (create_raw: false)
        # - bills_notes (create_raw: false)
        # - bills_sponsors (create_raw: false)

        print(f"✅ Found {len(config_based_tables)} related tables with raw data:")
        for table in config_based_tables:
            print(f"   - bills_{table}")

        return config_based_tables

    async def simulate_process_bills(self, limit: int | None = None) -> dict[str, any]:
        """Simulate the enhanced process_bills function."""
        print("🚀 Starting enhanced bills normalization pipeline")
        print("=" * 60)

        # Simulate raw bills data
        simulated_raw_bills = [
            {"source_doc_id": "hr1234-118", "data": {"bill": {"title": "Test Bill 1"}}},
            {"source_doc_id": "s567-118", "data": {"bill": {"title": "Test Bill 2"}}},
            {"source_doc_id": "hr890-118", "data": {"bill": {"title": "Test Bill 3"}}},
        ]

        if limit:
            simulated_raw_bills = simulated_raw_bills[:limit]

        print(f"📊 Found {len(simulated_raw_bills)} raw bills to process")

        # Initialize results tracking
        results = {
            "status": "in_progress",
            "total_bills": len(simulated_raw_bills),
            "processed_bills": 0,
            "failed_bills": 0,
            "main_records": 0,
            "extracted_tables": {},
            "related_data_processed": {},
        }

        # Process bills in batches with checkpointing
        batch_size = 2  # Small batch for demo
        bill_batches = [
            simulated_raw_bills[i : i + batch_size]
            for i in range(0, len(simulated_raw_bills), batch_size)
        ]

        print(
            f"\n📦 Processing {len(bill_batches)} batches of ~{batch_size} bills each"
        )

        for batch_idx, batch in enumerate(bill_batches):
            batch_id = f"bills_normalization_batch_{batch_idx}"
            print(
                f"\n🔄 Processing batch {batch_idx + 1}/{len(bill_batches)} (ID: {batch_id})"
            )

            # Simulate batch processing
            batch_results = await self._simulate_process_bills_batch(batch, batch_id)

            # Update results
            results["processed_bills"] += batch_results["processed"]
            results["failed_bills"] += batch_results["failed"]
            results["main_records"] += batch_results["main_records"]

            # Merge extracted tables counts
            for table, count in batch_results["extracted_tables"].items():
                results["extracted_tables"][table] = (
                    results["extracted_tables"].get(table, 0) + count
                )

            print(
                f"✅ Completed batch {batch_idx + 1}: {batch_results['processed']} processed, {batch_results['failed']} failed"
            )

        # Process all related raw data
        print(f"\n🔗 Processing related raw data tables...")
        related_results = await self._simulate_process_all_related_data()

        # Merge related data results
        for table, count in related_results.items():
            results["related_data_processed"][table] = (
                results["related_data_processed"].get(table, 0) + count
            )

        # Final results
        results["status"] = "success"

        return results

    async def _simulate_process_bills_batch(
        self, batch: list, batch_id: str
    ) -> dict[str, any]:
        """Simulate processing a batch of bills."""
        batch_results = {
            "processed": 0,
            "failed": 0,
            "main_records": 0,
            "extracted_tables": {},
        }

        for bill_data in batch:
            bill_id = bill_data.get("source_doc_id")
            print(f"   📄 Processing bill: {bill_id}")

            # Simulate processing time
            await asyncio.sleep(0.1)

            # Simulate successful processing
            batch_results["processed"] += 1
            batch_results["main_records"] += 1

            # Simulate extracted tables from list fields
            simulated_extracted = {
                "bills_sponsors": 1,
                "bills_laws": 2,
                "bills_cbocostestimates": 1,
            }

            for table_name, count in simulated_extracted.items():
                batch_results["extracted_tables"][table_name] = (
                    batch_results["extracted_tables"].get(table_name, 0) + count
                )

        return batch_results

    async def _simulate_process_all_related_data(self) -> dict[str, int]:
        """Simulate processing all related raw data tables."""
        # Get related tables from configuration dynamically
        related_tables = self._get_related_tables_with_raw_data()

        results = {}

        for table_suffix in related_tables:
            print(f"   🔄 Processing bills_{table_suffix}_raw...")

            # Simulate processing time
            await asyncio.sleep(0.1)

            # Simulate different record counts for different table types
            simulated_counts = {
                "actions": 25,
                "cosponsors": 15,
                "subjects": 8,
                "summaries": 3,
                "texts": 5,
                "titles": 4,
                "relatedbills": 2,
            }

            count = simulated_counts.get(table_suffix, 1)
            results[f"bills_{table_suffix}"] = count
            print(f"   ✅ Processed {count} records from bills_{table_suffix}_raw")

        return results


async def demo():
    """Run the enhanced normalizer demo."""
    print("🎯 Enhanced Bills Database Normalizer Demo")
    print("=" * 60)
    print("This demo shows improvements:")
    print("1. ✅ Config-based related table discovery (no hardcoding)")
    print("2. ✅ Batched processing with checkpointing")
    print("3. ✅ Comprehensive progress tracking")
    print("4. ✅ Processing of both bills and related data")
    print("5. ✅ Better error handling and resilience")
    print()

    # Create normalizer instance
    normalizer = EnhancedBillsNormalizerDemo()

    # Run processing simulation
    results = await normalizer.simulate_process_bills(limit=3)

    # Display final results
    print("\n" + "=" * 60)
    print("📊 FINAL PROCESSING RESULTS")
    print("=" * 60)
    print(f"Status: {results['status']}")
    print(f"Total Bills: {results['total_bills']}")
    print(f"Processed Bills: {results['processed_bills']}")
    print(f"Failed Bills: {results['failed_bills']}")
    print(f"Main Records Created: {results['main_records']}")

    print(f"\n📊 Extracted Tables (from bill nested lists):")
    for table, count in results["extracted_tables"].items():
        print(f"   {table}: {count} records")

    print(f"\n📊 Related Data Processed (from raw tables):")
    for table, count in results["related_data_processed"].items():
        print(f"   {table}: {count} records")

    total_records = (
        results["main_records"]
        + sum(results["extracted_tables"].values())
        + sum(results["related_data_processed"].values())
    )
    print(f"\n🎯 Total Records Processed: {total_records}")

    print("\n✨ Demo completed! The enhanced normalizer successfully:")
    print("   • Used configuration to discover related tables")
    print("   • Processed data in manageable batches")
    print("   • Handled both main bills and related data")
    print("   • Provided comprehensive progress tracking")


if __name__ == "__main__":
    asyncio.run(demo())
