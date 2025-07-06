#!/usr/bin/env python3
"""
Cleanup Script for Streamlined Architecture

This script removes unnecessary files that are no longer needed with the
streamlined 4-layer architecture, keeping only essential components.
"""

import os
import shutil
from pathlib import Path


def main():
    """Execute the cleanup process."""
    print("🧹 Starting Streamlined Architecture Cleanup")

    # Get project root
    project_root = Path(__file__).parent.parent
    streamlined_root = project_root / "src" / "streamlined"

    # Files to remove from libs
    libs_to_remove = [
        "checkpoint.py",  # Compatibility shim
        "data_type_config.py",  # Merged into registry
        "batch_accumulator.py",  # Replaced by integrated storage
        "record_processing_context.py",  # Simplified approach
    ]

    print("\n📁 Cleaning up libs folder...")
    libs_dir = streamlined_root / "libs"

    for file_name in libs_to_remove:
        file_path = libs_dir / file_name
        if file_path.exists():
            print(f"  ❌ Removing {file_path}")
            file_path.unlink()
        else:
            print(f"  ⚠️  {file_name} not found (already removed?)")

    # Update libs __init__.py to remove deleted imports
    libs_init = libs_dir / "__init__.py"
    if libs_init.exists():
        with open(libs_init, "r") as f:
            content = f.read()

        # Remove imports for deleted modules
        lines_to_remove = [
            "from .batch_accumulator import BatchAccumulator",
            "from .checkpoint import",
            "from .data_type_config import",
            "from .record_processing_context import",
            '"BatchAccumulator"',
            '"CheckpointManager"',
            '"DataTypeConfig"',
            '"RecordProcessingContext"',
        ]

        new_content = content
        for line in lines_to_remove:
            new_content = "\n".join(
                [l for l in new_content.split("\n") if line not in l]
            )

        with open(libs_init, "w") as f:
            f.write(new_content)

        print(f"  ✅ Updated {libs_init}")

    # Clean up data type folders
    print("\n📂 Cleaning up data type folders...")
    data_types_dir = (
        project_root / "src" / "bicam_collection" / "data_types" / "congressional"
    )

    files_to_remove = [
        "fetcher.py",
        "cleaner.py",
        "database_normalizer.py",
        "specialized_assets.py",
    ]

    # Don't remove base classes yet - they may still be needed for transition
    skip_dirs = ["base", "abstract"]

    for data_type_dir in data_types_dir.iterdir():
        if (
            not data_type_dir.is_dir()
            or data_type_dir.name.startswith("_")
            or data_type_dir.name in skip_dirs
        ):
            continue

        print(f"\n  📁 Cleaning {data_type_dir.name}...")

        for file_name in files_to_remove:
            file_path = data_type_dir / file_name
            if file_path.exists():
                print(f"    ❌ Removing {file_path}")
                file_path.unlink()
            else:
                print(f"    ⚠️  {file_name} not found")

        # Keep only config.yaml and __init__.py
        remaining_files = [
            f
            for f in data_type_dir.iterdir()
            if f.is_file()
            and f.name not in ["config.yaml", "__init__.py", "__pycache__"]
        ]
        if remaining_files:
            print(f"    ℹ️  Keeping: {[f.name for f in remaining_files]}")

        # Update __init__.py to remove deleted imports
        init_file = data_type_dir / "__init__.py"
        if init_file.exists():
            try:
                with open(init_file, "r") as f:
                    content = f.read()

                # Remove imports for deleted classes
                imports_to_remove = [
                    "from .fetcher import",
                    "from .cleaner import",
                    "from .database_normalizer import",
                    "from .specialized_assets import",
                ]

                lines = content.split("\n")
                new_lines = []

                for line in lines:
                    should_remove = any(imp in line for imp in imports_to_remove)
                    if not should_remove:
                        new_lines.append(line)

                # Keep only essential content
                essential_content = '''"""
{data_type} data type configuration.

This data type now uses the streamlined architecture with plugin-based processing.
All custom logic has been moved to plugins while configuration remains here.
"""

# This data type uses the streamlined plugin architecture
# Custom logic is now in: src/streamlined/plugins/congressional.py
'''.format(data_type=data_type_dir.name.title())

                with open(init_file, "w") as f:
                    f.write(essential_content)

                print(f"    ✅ Updated {init_file}")

            except Exception as e:
                print(f"    ⚠️  Could not update {init_file}: {e}")

    print("\n🎯 Summary of Changes:")
    print("  ✅ Removed redundant libs files")
    print("  ✅ Cleaned up data type folders")
    print("  ✅ Updated import statements")
    print("  ⚠️  Base classes kept for transition period")
    print("\n📋 Next Steps:")
    print("  1. Test the streamlined architecture with existing data")
    print("  2. Verify plugins work correctly")
    print("  3. Remove base classes once transition is complete")
    print("  4. Update any remaining import statements")
    print("\n🚀 Streamlined architecture cleanup complete!")


if __name__ == "__main__":
    main()
