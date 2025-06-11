"""Command-line interface for BICAM Collection."""

import argparse
import asyncio
import sys
from typing import Optional
import logging

from bicam_collection.core.config import settings
from bicam_collection.core.exceptions import BicamError


def setup_logging(level: str = "INFO") -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )


def create_parser() -> argparse.ArgumentParser:
    """Create the main argument parser."""
    parser = argparse.ArgumentParser(
        prog="bicam",
        description="BICAM Collection - Congressional Data ELT Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  bicam extract congressional --data-types bills,committees
  bicam transform --data-type bills --resume-from clean_bills
  bicam load csv --output-dir ./exports --schema bicam
  bicam config validate
        """.strip()
    )
    
    parser.add_argument(
        "--version",
        action="version", 
        version="%(prog)s 0.2.0"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level"
    )
    
    parser.add_argument(
        "--config-file",
        type=str,
        help="Path to configuration file"
    )
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Extract command
    extract_parser = subparsers.add_parser("extract", help="Extract data from APIs")
    extract_subparsers = extract_parser.add_subparsers(dest="source", help="Data source")
    
    # Congressional extraction
    congress_parser = extract_subparsers.add_parser("congressional", help="Extract Congressional data")
    congress_parser.add_argument(
        "--data-types", 
        type=str,
        help="Comma-separated list of data types to extract (bills,committees,members,etc.)"
    )
    congress_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for processing"
    )
    congress_parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for extraction (YYYY-MM-DD)"
    )
    
    # GovInfo extraction  
    govinfo_parser = extract_subparsers.add_parser("govinfo", help="Extract GovInfo data")
    govinfo_parser.add_argument(
        "--collections",
        type=str,
        help="Comma-separated list of collections to extract"
    )
    govinfo_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for processing"
    )
    
    # Transform command
    transform_parser = subparsers.add_parser("transform", help="Transform and clean data")
    transform_parser.add_argument(
        "--data-type",
        type=str,
        required=True,
        help="Data type to transform"
    )
    transform_parser.add_argument(
        "--resume-from",
        type=str,
        help="Module to resume processing from"
    )
    transform_parser.add_argument(
        "--schema",
        type=str,
        default="congressional",
        choices=["congressional", "govinfo"],
        help="Target schema"
    )
    
    # Load command
    load_parser = subparsers.add_parser("load", help="Load data to destinations")
    load_subparsers = load_parser.add_subparsers(dest="destination", help="Load destination")
    
    # CSV export
    csv_parser = load_subparsers.add_parser("csv", help="Export to CSV files")
    csv_parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Output directory for CSV files"
    )
    csv_parser.add_argument(
        "--schema",
        type=str,
        required=True,
        help="Database schema to export"
    )
    csv_parser.add_argument(
        "--compress",
        action="store_true",
        help="Compress output files"
    )
    
    # Config command
    config_parser = subparsers.add_parser("config", help="Configuration management")
    config_subparsers = config_parser.add_subparsers(dest="config_action", help="Config actions")
    
    config_subparsers.add_parser("validate", help="Validate configuration")
    config_subparsers.add_parser("show", help="Show current configuration")
    
    return parser


async def handle_extract_congressional(args) -> int:
    """Handle congressional data extraction."""
    print(f"Extracting Congressional data: {args.data_types}")
    print(f"Batch size: {args.batch_size}")
    if args.start_date:
        print(f"Start date: {args.start_date}")
    
    # TODO: Implement actual extraction logic when modules are migrated
    print("Congressional extraction not yet implemented in new structure")
    return 0


async def handle_extract_govinfo(args) -> int:
    """Handle GovInfo data extraction."""
    print(f"Extracting GovInfo collections: {args.collections}")
    print(f"Batch size: {args.batch_size}")
    
    # TODO: Implement actual extraction logic when modules are migrated
    print("GovInfo extraction not yet implemented in new structure")
    return 0


async def handle_transform(args) -> int:
    """Handle data transformation."""
    print(f"Transforming data type: {args.data_type}")
    print(f"Target schema: {args.schema}")
    if args.resume_from:
        print(f"Resuming from: {args.resume_from}")
    
    # TODO: Implement actual transformation logic when modules are migrated
    print("Transformation not yet implemented in new structure")
    return 0


async def handle_load_csv(args) -> int:
    """Handle CSV export."""
    print(f"Exporting schema '{args.schema}' to: {args.output_dir}")
    if args.compress:
        print("Compression enabled")
    
    # TODO: Implement actual CSV export logic when modules are migrated
    print("CSV export not yet implemented in new structure")
    return 0


def handle_config_validate(args) -> int:
    """Validate configuration."""
    try:
        if settings is None:
            print("✗ Configuration validation failed: Settings could not be initialized")
            return 1
            
        # Test database config
        if settings.database is None:
            print("✗ Database config invalid or missing required environment variables")
        else:
            db_config = settings.database
            print(f"✓ Database config valid - Host: {db_config.host}:{db_config.port}")
        
        # Test API config
        if settings.api is None:
            print("✗ API config invalid or missing required environment variables")
        else:
            api_config = settings.api
            print(f"✓ API config valid - Congress keys: {len(api_config.congress_api_keys)}")
            print(f"✓ GovInfo key configured: {'Yes' if api_config.govinfo_api_key else 'No'}")
        
        if settings.database and settings.api:
            print("✓ Configuration validation passed")
            return 0
        else:
            print("✗ Configuration validation failed: Some required configs are missing")
            return 1
        
    except Exception as e:
        print(f"✗ Configuration validation failed: {e}")
        return 1


def handle_config_show(args) -> int:
    """Show current configuration."""
    if settings is None:
        print("Configuration could not be loaded")
        return 1
        
    print("Current Configuration:")
    print(f"  Environment: {settings.environment}")
    print(f"  Debug: {settings.debug}")
    print()
    
    print("Database:")
    if settings.database:
        print(f"  Host: {settings.database.host}")
        print(f"  Port: {settings.database.port}")
        print(f"  Database: {settings.database.database}")
        print(f"  User: {settings.database.user}")
    else:
        print("  ✗ Not configured (missing environment variables)")
    print()
    
    print("API:")
    if settings.api:
        print(f"  Congress API Keys: {len(settings.api.congress_api_keys)} configured")
        print(f"  GovInfo API Key: {'✓' if settings.api.govinfo_api_key else '✗'}")
        print(f"  Rate Limit: {settings.api.rate_limit} req/sec")
    else:
        print("  ✗ Not configured (missing environment variables)")
    print()
    
    print("Processing:")
    if settings.processing:
        print(f"  Batch Size: {settings.processing.batch_size}")
        print(f"  Max Workers: {settings.processing.max_workers}")
        print(f"  Parallel Processing: {settings.processing.enable_parallel}")
    else:
        print("  ✗ Not configured")
    
    return 0


async def main(argv: Optional[list] = None) -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args(argv)
    
    # Set up logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Handle commands
        if args.command == "extract":
            if args.source == "congressional":
                return await handle_extract_congressional(args)
            elif args.source == "govinfo":
                return await handle_extract_govinfo(args)
            else:
                parser.error("Extract command requires a source (congressional or govinfo)")
                
        elif args.command == "transform":
            return await handle_transform(args)
            
        elif args.command == "load":
            if args.destination == "csv":
                return await handle_load_csv(args)
            else:
                parser.error("Load command requires a destination (csv)")
                
        elif args.command == "config":
            if args.config_action == "validate":
                return handle_config_validate(args)
            elif args.config_action == "show":
                return handle_config_show(args)
            else:
                parser.error("Config command requires an action (validate or show)")
        else:
            parser.print_help()
            return 1
            
    except BicamError as e:
        logger.error(f"BICAM error: {e}")
        return 1
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


def cli_main() -> None:
    """CLI entry point for setuptools."""
    sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    cli_main() 