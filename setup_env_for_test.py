#!/usr/bin/env python3
"""
Environment Setup Helper for Bills Fetching Test

This script helps set up the environment for testing bills fetching
with the streamlined architecture.
"""

import os
import sys
from pathlib import Path


def setup_test_environment():
    """Set up environment variables for testing."""
    print("🔧 Setting up test environment...")

    # Database settings (using default test values)
    env_vars = {
        "POSTGRESQL_HOST": "localhost",
        "POSTGRESQL_PORT": "5432",
        "POSTGRESQL_DATABASE": "bicam_collection_test",
        "POSTGRESQL_USERNAME": "postgres",
        "POSTGRESQL_PASSWORD": "password",
        # API Keys (you'll need to provide real ones for actual testing)
        "CONGRESSIONAL_API_KEY": "your_congressional_api_key_here",
        "GOVINFO_API_KEY": "your_govinfo_api_key_here",
        # Processing settings
        "BATCH_SIZE": "10",
        "MAX_WORKERS": "2",
        "CHUNK_SIZE": "100",
        "PAGE_SIZE": "25",
        "MAX_CONCURRENT": "2",
        # Environment settings
        "ENVIRONMENT": "test",
        "DEBUG": "true",
        "LOG_LEVEL": "INFO",
        "INCREMENTAL": "false",
        "FALLBACK_DAYS": "7",
    }

    # Set environment variables
    for key, value in env_vars.items():
        if not os.getenv(key):
            os.environ[key] = value
            print(f"✅ Set {key} = {value}")
        else:
            print(f"⏭️  {key} already set = {os.getenv(key)}")

    # Check for critical environment variables
    print("\n🔍 Checking critical environment variables...")

    critical_vars = [
        "CONGRESSIONAL_API_KEY",
        "POSTGRESQL_HOST",
        "POSTGRESQL_DATABASE",
        "POSTGRESQL_USERNAME",
        "POSTGRESQL_PASSWORD",
    ]

    all_set = True
    for var in critical_vars:
        value = os.getenv(var)
        if value and value != "your_congressional_api_key_here":
            print(f"✅ {var} is set")
        else:
            print(f"⚠️  {var} needs to be set for full testing")
            if var == "CONGRESSIONAL_API_KEY":
                all_set = False

    if all_set:
        print("\n🎉 All critical environment variables are set!")
        print("You can now run the bills fetching test with real API calls.")
    else:
        print("\n⚠️  Some environment variables need to be set.")
        print("The test will run in simulation mode without real API calls.")
        print("\nTo enable real API calls, set:")
        print("  export CONGRESSIONAL_API_KEY=your_actual_api_key")
        print("  export POSTGRESQL_HOST=your_database_host")
        print("  export POSTGRESQL_USERNAME=your_database_username")
        print("  export POSTGRESQL_PASSWORD=your_database_password")

    return all_set


def create_env_file():
    """Create a .env file for testing."""
    print("\n📝 Creating .env file...")

    env_content = """# Test Environment Variables for Bills Fetching
# Copy and modify these values for your setup

# Database Configuration
POSTGRESQL_HOST=localhost
POSTGRESQL_PORT=5432
POSTGRESQL_DATABASE=bicam_collection_test
POSTGRESQL_USERNAME=postgres
POSTGRESQL_PASSWORD=password

# API Keys (REQUIRED for real testing)
CONGRESSIONAL_API_KEY=your_congressional_api_key_here
GOVINFO_API_KEY=your_govinfo_api_key_here

# Processing Configuration
BATCH_SIZE=10
MAX_WORKERS=2
CHUNK_SIZE=100
PAGE_SIZE=25
MAX_CONCURRENT=2

# Environment Settings
ENVIRONMENT=test
DEBUG=true
LOG_LEVEL=INFO
INCREMENTAL=false
FALLBACK_DAYS=7

# Test specific settings
PROCESSING_ENABLE_PARALLEL=false
PROCESSING_CHUNK_SIZE=50
PROCESSING_MAX_WORKERS=2
"""

    env_file = Path(".env")
    if env_file.exists():
        print(f"⏭️  .env file already exists at {env_file}")
    else:
        env_file.write_text(env_content)
        print(f"✅ Created .env file at {env_file}")
        print("📝 Please edit this file and add your actual API keys!")

    return env_file


def main():
    """Main setup function."""
    print("🎯 Bills Fetching Test - Environment Setup")
    print("=" * 50)

    # Setup environment
    setup_test_environment()

    # Create env file
    create_env_file()

    print("\n🚀 Ready to run bills fetching test!")
    print("\nNext steps:")
    print("1. Edit .env file with your actual API keys")
    print("2. Run: python test_bills_fetching_streamlined.py")
    print("\nFor basic testing without API calls:")
    print("   python test_bills_fetching_streamlined.py")


if __name__ == "__main__":
    main()
