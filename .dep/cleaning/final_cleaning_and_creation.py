"""Final data merging and schema creation for bicam-collection.

This module implements Phase 4 of the 4-phase pipeline:
- Merges Congressional and GovInfo data for each data type
- Creates final bicam schema with proper primary keys, foreign keys, and indices
- Handles data coalescing and deduplication
- Implements comprehensive data validation and quality checks
"""

import logging
import argparse
from datatypes import (
    amendments,
    bills,
    committeemeetings,
    committees,
    committeeprints,
    committeereports,
    congresses,
    hearings,
    members,
    nominations,
    treaties,
    congressional_directories,
    bill_collections,
    committee_prints,
    committee_reports,
    congressional_hearings,
    treaty_docs,
)
from cleaning_coordinator import CleaningCoordinator
from dotenv import load_dotenv
import os
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2 import sql as _psql
from psycopg2.extras import RealDictCursor

from bicam_collection.cleaning.cleaning_utils.insert_data import write_data
from bicam_collection.db.database_setup import DatabaseManager

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_phase(
    modules: list,
    coordinator: CleaningCoordinator,
    start_from: str = None,
    phase_name: str = "",
) -> bool:
    """Process a single phase of modules"""
    coordinator.cleaning_modules = []
    coordinator.dependency_graph = None

    logger.info(f"\n{'=' * 20} Processing {phase_name} {'=' * 20}")

    # Register modules for this phase
    for name, func, schema in modules:
        coordinator.register_module(name, func, schema)

    coordinator.initialize()

    if start_from:
        phase_names = [name for name, _, _ in modules]
        if start_from not in phase_names:
            return False
        start_idx = phase_names.index(start_from)
        modules = modules[start_idx:]

    # Map special cases for verification
    table_name_map = {
        "committees_base": "committees",
        "committees_relationships": "committees",
    }

    for name, func, schema in modules:
        try:
            logger.info(f"Processing module {name}...")
            func(coordinator=coordinator)

            main_table = name.lstrip("_")
            # Use mapping for verification
            verify_table = table_name_map.get(main_table, main_table)

            count = coordinator.verify_table_population(schema, verify_table)
            if count == 0:
                raise Exception(f"No data was written to {schema}.{verify_table}")

            logger.info(
                f"Successfully completed {name} cleaning script with {count} rows"
            )

        except Exception as e:
            logger.error(f"Error in {name} cleaning script: {str(e)}")
            raise

    return start_from and True


def main():
    parser = argparse.ArgumentParser(description="Run all cleaning scripts")
    parser.add_argument("--resume-from", type=str, help="Module name to resume from")
    args = parser.parse_args()

    coordinator = CleaningCoordinator()

    # Level 1: No dependencies
    modules_phase1 = [
        ("congresses", congresses.clean_congresses, "congressional"),
    ]

    # Level 2: Depends only on congresses
    modules_phase2 = [
        ("members", members.clean_members, "congressional"),
        ("treaties", treaties.clean_treaties, "congressional"),
    ]

    # Level 3: Base committees table
    modules_phase3 = [
        (
            "committees_base",
            committees.clean_committees_base,
            "congressional",
        ),  # New function for base table only
    ]

    # Level 4A: Dependencies on committees_base
    modules_phase4a = [
        ("bills", bills.clean_bills, "congressional"),
        ("hearings", hearings.clean_hearings, "congressional"),
    ]

    # Level 4B: Dependencies on 4A
    modules_phase4b = [
        ("nominations", nominations.clean_nominations, "congressional"),
    ]
    # Level 5: Dependencies on bills and earlier
    modules_phase5 = [
        ("amendments", amendments.clean_amendments, "congressional"),
        ("committeereports", committeereports.clean_committeereports, "congressional"),
        ("committeeprints", committeeprints.clean_committeeprints, "congressional"),
    ]

    # Level 6: Final dependencies and relationship tables
    modules_phase6 = [
        (
            "committees_relationships",
            committees.clean_committees_relationships,
            "congressional",
        ),  # New function for relationship tables
    ]

    modules_phase7 = [
        (
            "committeemeetings",
            committeemeetings.clean_committeemeetings,
            "congressional",
        ),
    ]

    modules_phase8 = [
        (
            "congressional_directories",
            congressional_directories.clean_congressional_directories,
            "govinfo",
        ),
        ("bill_collections", bill_collections.clean_bill_collections, "govinfo"),
        ("committee_prints", committee_prints.clean_committee_prints, "govinfo"),
        ("committee_reports", committee_reports.clean_committee_reports, "govinfo"),
        ("congressional_hearings", congressional_hearings.clean_hearings, "govinfo"),
        ("treaty_docs", treaty_docs.clean_treaties, "govinfo"),
    ]

    phases = [
        ("Phase 1 - No Dependencies", modules_phase1),
        ("Phase 2 - Congress Dependencies", modules_phase2),
        ("Phase 3 - Base Committees", modules_phase3),
        ("Phase 4A - Primary Tables", modules_phase4a),
        ("Phase 4B - Dependencies on 4A", modules_phase4b),
        ("Phase 5 - Secondary Dependencies", modules_phase5),
        ("Phase 6 - Final Dependencies", modules_phase6),
        ("Phase 7 - Committee Meetings", modules_phase7),
        ("Phase 8 - GovInfo", modules_phase8),
    ]

    found_module = False
    for phase_name, modules in phases:
        if found_module:
            process_phase(modules, coordinator, phase_name=phase_name)
        else:
            found_module = process_phase(
                modules, coordinator, args.resume_from, phase_name
            )


if __name__ == "__main__":
    main()

    # TODO LIST:
# 1: final cleaning for congressional
# 4: link up govinfo/congressional into BICAM
# 4.5: RESCRAPE SPOTCHECK SCRIPT
# 5: export zips
# 6: finish lobbying matching
# 7: clean/port over code to github
### Processing order notes:
# standardize dates
# remove tags
# add raw texts
# redo counts for tables
# expand ids into parts in main tables
# make script to join the two
# rescrape congressional directories


# make spot check for committee bills/reports: hsru00, hsvr00
# hsag00
# hsap00
# hsju00
# hswm00
# slin00
# ssap00
# ssas00
# ssfi00
# ssfr00
# ssju00
# ssra00
# jslc00
# jspr00
# ssva00
# ssbu00
# scnc00
# hsbu00
# hsas29
# hsvr09
# hsha27
# hswm04
# hswm06
# hsju13
# hswm02
# hsap24
# hlfd00
# hsvr08
# hswm01
# hsba21
# hsvr11
# hlzs00
# hsvr10


# FULL CHECKS WITHOUT SUBCOMMITTEES
# hsap23


# establishing authority link for committee history


# ============================================================================
# DATA TYPE MERGING CONFIGURATIONS
# ============================================================================

# Data types that exist in both Congressional and GovInfo
MERGEABLE_DATA_TYPES = {
    "bills": {
        "congressional_table": "bicam_congressional.bills",
        "govinfo_table": "bicam_govinfo.bill_collections",
        "merge_key": "bill_id",
        "final_table": "bicam_final.bills",
        "coalesce_fields": {
            "is_appropriation": "govinfo",  # Prefer GovInfo for this field
            "is_private": "govinfo",
            "pages": "govinfo",
            "current_chamber": "govinfo",
            "version_code": "govinfo",
        },
    },
    "hearings": {
        "congressional_table": "bicam_congressional.hearings",
        "govinfo_table": "bicam_govinfo.hearings",
        "merge_key": "hearing_id",
        "final_table": "bicam_final.hearings",
        "coalesce_fields": {
            "pages": "govinfo",
            "is_appropriation": "govinfo",
        },
    },
    "treaties": {
        "congressional_table": "bicam_congressional.treaties",
        "govinfo_table": "bicam_govinfo.treaties",
        "merge_key": "treaty_id",
        "final_table": "bicam_final.treaties",
        "coalesce_fields": {
            "pages": "govinfo",
            "summary": "govinfo",
        },
    },
    "committee_prints": {
        "congressional_table": "bicam_congressional.committeeprints",
        "govinfo_table": "bicam_govinfo.committee_prints",
        "merge_key": "print_id",
        "final_table": "bicam_final.committeeprints",
        "coalesce_fields": {
            "pages": "govinfo",
        },
    },
    "committee_reports": {
        "congressional_table": "bicam_congressional.committeereports",
        "govinfo_table": "bicam_govinfo.committee_reports",
        "merge_key": "report_id",
        "final_table": "bicam_final.committeereports",
        "coalesce_fields": {
            "pages": "govinfo",
        },
    },
}

# Congressional-only data types
CONGRESSIONAL_ONLY_TYPES = [
    "amendments",
    "members",
    "committees",
    "committeemeetings",
    "nominations",
    "congresses",
]

# GovInfo-only data types
GOVINFO_ONLY_TYPES = ["congressional_directories"]

# ============================================================================
# CORE MERGING FUNCTIONS
# ============================================================================


def merge_data_type(data_type: str) -> Dict[str, Any]:
    """Merge Congressional and GovInfo data for a specific data type.

    Args:
        data_type: The data type to merge (e.g., 'bills', 'hearings')

    Returns:
        Dict containing merge results and statistics
    """

    logger.info(f"Starting final merge for {data_type}")

    try:
        if data_type in MERGEABLE_DATA_TYPES:
            return merge_dual_source_data_type(data_type)
        elif data_type in CONGRESSIONAL_ONLY_TYPES:
            return merge_congressional_only_data_type(data_type)
        elif data_type in GOVINFO_ONLY_TYPES:
            return merge_govinfo_only_data_type(data_type)
        else:
            logger.warning(f"Unknown data type for merging: {data_type}")
            return {
                "data_type": data_type,
                "merged_records": 0,
                "final_tables": 0,
                "success": False,
                "error": f"Unknown data type: {data_type}",
            }

    except Exception as e:
        logger.error(f"Failed to merge {data_type}: {e}")
        return {
            "data_type": data_type,
            "merged_records": 0,
            "final_tables": 0,
            "success": False,
            "error": str(e),
        }


def merge_dual_source_data_type(data_type: str) -> Dict[str, Any]:
    """Merge data type that exists in both Congressional and GovInfo."""

    config = MERGEABLE_DATA_TYPES[data_type]

    conn = get_database_connection()

    try:
        # Read data from both sources
        congressional_df = read_table_to_dataframe(conn, config["congressional_table"])
        govinfo_df = read_table_to_dataframe(conn, config["govinfo_table"])

        logger.info(f"Read {len(congressional_df)} Congressional {data_type} records")
        logger.info(f"Read {len(govinfo_df)} GovInfo {data_type} records")

        # Perform intelligent merge
        merged_df = intelligent_merge(
            congressional_df, govinfo_df, config["merge_key"], config["coalesce_fields"]
        )

        # Write to final schema
        write_data(
            merged_df,
            config["final_table"],
            key_columns=[config["merge_key"]],
            if_exists="truncate",
        )

        # Create metadata tables if they exist
        metadata_tables = create_metadata_tables(
            conn, data_type, congressional_df, govinfo_df
        )

        # Create related tables
        related_tables = merge_related_tables(conn, data_type)

        total_tables = 1 + metadata_tables + related_tables

        logger.info(
            f"Successfully merged {len(merged_df)} {data_type} records into {total_tables} final tables"
        )

        return {
            "data_type": data_type,
            "merged_records": len(merged_df),
            "congressional_records": len(congressional_df),
            "govinfo_records": len(govinfo_df),
            "final_tables": total_tables,
            "success": True,
        }

    finally:
        conn.close()


def merge_congressional_only_data_type(data_type: str) -> Dict[str, Any]:
    """Merge data type that exists only in Congressional data."""

    conn = get_database_connection()

    try:
        # Read Congressional data
        source_table = f"bicam_congressional.{data_type}"
        final_table = f"bicam_final.{data_type}"

        df = read_table_to_dataframe(conn, source_table)

        if df.empty:
            logger.warning(f"No data found in {source_table}")
            return {
                "data_type": data_type,
                "merged_records": 0,
                "final_tables": 0,
                "success": True,
            }

        # Get primary key from schema
        primary_key = get_primary_key_columns(conn, "bicam_congressional", data_type)

        # Copy to final schema
        write_data(
            df,
            final_table,
            key_columns=primary_key,
            if_exists="truncate",
        )

        # Copy related tables
        related_tables = copy_related_tables(conn, data_type, "congressional")

        total_tables = 1 + related_tables

        logger.info(
            f"Successfully copied {len(df)} {data_type} records to final schema"
        )

        return {
            "data_type": data_type,
            "merged_records": len(df),
            "congressional_records": len(df),
            "govinfo_records": 0,
            "final_tables": total_tables,
            "success": True,
        }

    finally:
        conn.close()


def merge_govinfo_only_data_type(data_type: str) -> Dict[str, Any]:
    """Merge data type that exists only in GovInfo data."""

    conn = get_database_connection()

    try:
        # Read GovInfo data
        source_table = f"bicam_govinfo.{data_type}"
        final_table = f"bicam_final.{data_type}"

        df = read_table_to_dataframe(conn, source_table)

        if df.empty:
            logger.warning(f"No data found in {source_table}")
            return {
                "data_type": data_type,
                "merged_records": 0,
                "final_tables": 0,
                "success": True,
            }

        # Get primary key from schema
        primary_key = get_primary_key_columns(conn, "bicam_govinfo", data_type)

        # Copy to final schema
        write_data(
            df,
            final_table,
            key_columns=primary_key,
            if_exists="truncate",
        )

        # Copy related tables
        related_tables = copy_related_tables(conn, data_type, "govinfo")

        total_tables = 1 + related_tables

        logger.info(
            f"Successfully copied {len(df)} {data_type} records to final schema"
        )

        return {
            "data_type": data_type,
            "merged_records": len(df),
            "congressional_records": 0,
            "govinfo_records": len(df),
            "final_tables": total_tables,
            "success": True,
        }

    finally:
        conn.close()


# ============================================================================
# INTELLIGENT MERGING LOGIC
# ============================================================================


def intelligent_merge(
    congressional_df: pd.DataFrame,
    govinfo_df: pd.DataFrame,
    merge_key: str,
    coalesce_fields: Dict[str, str],
) -> pd.DataFrame:
    """Perform intelligent merge with field-specific coalescing preferences."""

    # Ensure merge key exists in both dataframes
    if merge_key not in congressional_df.columns:
        logger.warning(f"Merge key {merge_key} not found in Congressional data")
        return govinfo_df

    if merge_key not in govinfo_df.columns:
        logger.warning(f"Merge key {merge_key} not found in GovInfo data")
        return congressional_df

    # Perform outer join to get all records
    merged_df = pd.merge(
        congressional_df,
        govinfo_df,
        on=merge_key,
        how="outer",
        suffixes=("_congressional", "_govinfo"),
    )

    # Apply coalescing logic for each field
    final_df = merged_df[[merge_key]].copy()

    # Get all unique column names (without suffixes)
    all_columns = set()
    for col in merged_df.columns:
        if col.endswith("_congressional"):
            all_columns.add(col[:-14])  # Remove "_congressional"
        elif col.endswith("_govinfo"):
            all_columns.add(col[:-8])  # Remove "_govinfo"
        elif col == merge_key:
            continue
        else:
            all_columns.add(col)

    # Process each column with intelligent coalescing
    for col in all_columns:
        congressional_col = f"{col}_congressional"
        govinfo_col = f"{col}_govinfo"

        # Determine preference from configuration
        preference = coalesce_fields.get(
            col, "congressional"
        )  # Default to congressional

        if congressional_col in merged_df.columns and govinfo_col in merged_df.columns:
            if preference == "govinfo":
                # Prefer GovInfo, fallback to Congressional
                final_df[col] = merged_df[govinfo_col].fillna(
                    merged_df[congressional_col]
                )
            else:
                # Prefer Congressional, fallback to GovInfo
                final_df[col] = merged_df[congressional_col].fillna(
                    merged_df[govinfo_col]
                )
        elif congressional_col in merged_df.columns:
            final_df[col] = merged_df[congressional_col]
        elif govinfo_col in merged_df.columns:
            final_df[col] = merged_df[govinfo_col]
        elif col in merged_df.columns:
            final_df[col] = merged_df[col]

    logger.info(
        f"Merged {len(congressional_df)} Congressional + {len(govinfo_df)} GovInfo = {len(final_df)} final records"
    )

    return final_df


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def get_database_connection():
    """Get database connection using environment variables."""
    return psycopg2.connect(
        host=os.getenv("POSTGRESQL_HOST"),
        port=os.getenv("POSTGRESQL_PORT"),
        database=os.getenv("POSTGRESQL_DATABASE"),
        user=os.getenv("POSTGRESQL_USERNAME"),
        password=os.getenv("POSTGRESQL_PASSWORD"),
    )


def read_table_to_dataframe(conn, table_name: str) -> pd.DataFrame:
    """Read a database table into a pandas DataFrame."""
    try:
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, conn)
    except Exception as e:
        logger.warning(f"Could not read table {table_name}: {e}")
        return pd.DataFrame()


def get_primary_key_columns(conn, schema: str, table: str) -> List[str]:
    """Get primary key columns for a table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            ORDER BY a.attnum
        """,
            (f"{schema}.{table}",),
        )

        return [row[0] for row in cur.fetchall()]


def create_metadata_tables(
    conn, data_type: str, congressional_df: pd.DataFrame, govinfo_df: pd.DataFrame
) -> int:
    """Create metadata tables linking Congressional and GovInfo records."""

    metadata_tables_created = 0

    # Create metadata table if GovInfo has package_id
    if "package_id" in govinfo_df.columns:
        try:
            # Create metadata linking table
            metadata_df = govinfo_df[["package_id"]].copy()

            # Add other metadata fields if they exist
            metadata_fields = [
                "su_doc_class_number",
                "migrated_doc_id",
                "govinfo_collection_code",
            ]
            for field in metadata_fields:
                if field in govinfo_df.columns:
                    metadata_df[field] = govinfo_df[field]

            # Write metadata table
            metadata_table = f"bicam_final.{data_type}_metadata"
            write_data(
                metadata_df,
                metadata_table,
                key_columns=["package_id"],
                if_exists="truncate",
            )

            metadata_tables_created += 1
            logger.info(f"Created metadata table {metadata_table}")

        except Exception as e:
            logger.warning(f"Failed to create metadata table for {data_type}: {e}")

    return metadata_tables_created


def merge_related_tables(conn, data_type: str) -> int:
    """Merge related tables for a data type."""

    related_tables_merged = 0

    # Get list of related tables from both schemas
    congressional_related = get_related_tables(conn, "bicam_congressional", data_type)
    govinfo_related = get_related_tables(conn, "bicam_govinfo", data_type)

    # Merge each related table
    all_related = set(congressional_related + govinfo_related)

    for related_table in all_related:
        try:
            merge_single_related_table(conn, data_type, related_table)
            related_tables_merged += 1
        except Exception as e:
            logger.warning(f"Failed to merge related table {related_table}: {e}")

    return related_tables_merged


def get_related_tables(conn, schema: str, data_type: str) -> List[str]:
    """Get list of related tables for a data type in a schema."""

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name LIKE %s
            AND table_name != %s
            ORDER BY table_name
        """,
            (schema, f"{data_type}_%", data_type),
        )

        return [row[0] for row in cur.fetchall()]


def merge_single_related_table(conn, data_type: str, related_table: str):
    """Merge a single related table from both sources."""

    congressional_table = f"bicam_congressional.{related_table}"
    govinfo_table = f"bicam_govinfo.{related_table}"
    final_table = f"bicam_final.{related_table}"

    # Read data from both sources
    congressional_df = read_table_to_dataframe(conn, congressional_table)
    govinfo_df = read_table_to_dataframe(conn, govinfo_table)

    # Combine dataframes
    if not congressional_df.empty and not govinfo_df.empty:
        # Union the data (assuming same schema)
        combined_df = pd.concat([congressional_df, govinfo_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates()
    elif not congressional_df.empty:
        combined_df = congressional_df
    elif not govinfo_df.empty:
        combined_df = govinfo_df
    else:
        return  # No data to merge

    # Get primary key for the table
    primary_key = get_primary_key_columns(conn, "bicam_congressional", related_table)
    if not primary_key:
        primary_key = get_primary_key_columns(conn, "bicam_govinfo", related_table)

    # Write to final schema
    write_data(
        combined_df,
        final_table,
        key_columns=primary_key,
        if_exists="truncate",
    )

    logger.info(f"Merged related table {related_table}: {len(combined_df)} records")


def copy_related_tables(conn, data_type: str, source: str) -> int:
    """Copy related tables from a single source to final schema."""

    source_schema = f"bicam_{source}"
    related_tables = get_related_tables(conn, source_schema, data_type)

    copied_tables = 0

    for related_table in related_tables:
        try:
            source_table = f"{source_schema}.{related_table}"
            final_table = f"bicam_final.{related_table}"

            df = read_table_to_dataframe(conn, source_table)

            if not df.empty:
                primary_key = get_primary_key_columns(
                    conn, source_schema, related_table
                )

                write_data(
                    df,
                    final_table,
                    key_columns=primary_key,
                    if_exists="truncate",
                )

                copied_tables += 1
                logger.info(f"Copied related table {related_table}: {len(df)} records")

        except Exception as e:
            logger.warning(f"Failed to copy related table {related_table}: {e}")

    return copied_tables


# ============================================================================
# VALIDATION AND QUALITY CHECKS
# ============================================================================


def validate_final_data(data_type: str) -> Dict[str, Any]:
    """Validate final merged data quality."""

    conn = get_database_connection()

    try:
        final_table = f"bicam_final.{data_type}"

        # Basic counts
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {final_table}")
            total_records = cur.fetchone()[0]

            # Check for nulls in primary key
            primary_key = get_primary_key_columns(conn, "bicam_final", data_type)
            if primary_key:
                null_check = " OR ".join([f"{col} IS NULL" for col in primary_key])
                cur.execute(f"SELECT COUNT(*) FROM {final_table} WHERE {null_check}")
                null_primary_keys = cur.fetchone()[0]
            else:
                null_primary_keys = 0

        quality_score = 1.0 - (null_primary_keys / max(total_records, 1))

        return {
            "data_type": data_type,
            "total_records": total_records,
            "null_primary_keys": null_primary_keys,
            "quality_score": quality_score,
            "validation_passed": quality_score >= 0.95,
        }

    finally:
        conn.close()
