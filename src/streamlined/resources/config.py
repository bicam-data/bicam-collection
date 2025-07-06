"""
Configuration for Streamlined Pipeline

This module provides clean configuration management for the streamlined pipeline,
with sensible defaults and clear organization.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Optional .env file support
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


@dataclass
class DatabaseConfig:
    """Database configuration."""

    host: str = "localhost"
    port: int = 5432
    database: str = "bicam_collection"
    username: str = "postgres"
    password: str = "password"
    min_size: int = 2
    max_size: int = 20
    command_timeout: int = 60

    def __post_init__(self):
        """Load database configuration from environment variables."""
        self.host = os.getenv("POSTGRESQL_HOST", self.host)
        self.port = int(os.getenv("POSTGRESQL_PORT", str(self.port)))
        self.database = os.getenv("POSTGRESQL_DATABASE", self.database)
        self.username = os.getenv("POSTGRESQL_USERNAME", self.username)
        self.password = os.getenv("POSTGRESQL_PASSWORD", self.password)

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        from urllib.parse import quote_plus

        username = quote_plus(self.username)
        password = quote_plus(self.password)
        return f"postgresql://{username}:{password}@{self.host}:{self.port}/{self.database}"


@dataclass
class APIConfig:
    """API configuration."""

    keys: list[str] = field(default_factory=list)
    rate_limit_per_second: float = 2.0
    max_retries: int = 3
    timeout: int = 30

    def __post_init__(self):
        """Load API keys from environment if not provided."""
        if not self.keys:
            # Try to load from environment variables
            congressional_keys = os.getenv("CONGRESSIONAL_API_KEY")
            if congressional_keys:
                self.keys.extend(
                    [k.strip() for k in congressional_keys.split(",") if k.strip()]
                )

            govinfo_keys = os.getenv("GOVINFO_API_KEY")
            if govinfo_keys:
                self.keys.extend(
                    [k.strip() for k in govinfo_keys.split(",") if k.strip()]
                )


@dataclass
class ProcessingConfig:
    """Processing configuration."""

    batch_size: int = 100
    max_workers: int = 4
    chunk_size: int = 5000
    page_size: int = 250
    max_concurrent: int = 5

    def __post_init__(self):
        """Load processing configuration from environment variables."""
        self.batch_size = int(os.getenv("BATCH_SIZE", str(self.batch_size)))
        self.max_workers = int(os.getenv("MAX_WORKERS", str(self.max_workers)))
        self.chunk_size = int(os.getenv("CHUNK_SIZE", str(self.chunk_size)))
        self.page_size = int(os.getenv("PAGE_SIZE", str(self.page_size)))
        self.max_concurrent = int(os.getenv("MAX_CONCURRENT", str(self.max_concurrent)))


@dataclass
class ParallelizationConfig:
    """Parallelization configuration."""

    enabled: bool = False
    fetcher: dict[str, Any] = field(default_factory=dict)
    normalizer: dict[str, Any] = field(default_factory=dict)
    cleaner: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Set default parallelization settings."""
        if self.enabled and not self.fetcher:
            self.fetcher = {
                "congressional": {
                    "num_sessions": 2,
                    "keys_per_session": 2,
                },
                "govinfo": {
                    "num_sessions": 2,
                    "keys_per_session": 2,
                },
            }


@dataclass
class InfrastructureConfig:
    """Infrastructure configuration."""

    checkpoint_db_path: str = str(
        Path(__file__).parent.parent.parent.parent.parent
        / "data"
        / "checkpoints"
        / "streamlined_checkpoints.db"
    )
    use_postgres_runs: bool = True
    use_postgres_checkpoints: bool = False
    use_optimized_storage: bool = True
    use_dynamic_pool: bool = False
    storage_flush_interval: int = 10
    storage_max_memory_mb: int = 500


@dataclass
class StreamlinedConfig:
    """Complete configuration for streamlined pipeline."""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    api: APIConfig = field(default_factory=APIConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    parallelization: ParallelizationConfig = field(
        default_factory=ParallelizationConfig
    )
    infrastructure: InfrastructureConfig = field(default_factory=InfrastructureConfig)

    # Processing parameters (runtime configurable)
    from_date: str | None = None
    to_date: str | None = None
    congress: int | None = None

    # Incremental processing flags
    incremental: bool = True
    fallback_days: int = 30
    use_checkpoint_resume: bool = False
    use_incremental_dates: bool = True
    rerun_mode: bool = False

    def __post_init__(self):
        """Load streamlined configuration from environment variables."""
        self.incremental = os.getenv("INCREMENTAL", "true").lower() == "true"
        self.fallback_days = int(os.getenv("FALLBACK_DAYS", "30"))

    @classmethod
    def from_env(cls, env_file: str | None = None) -> "StreamlinedConfig":
        """Create configuration from environment variables."""
        if load_dotenv:
            if env_file:
                # Load specified environment file
                load_dotenv(env_file, override=True)
            else:
                # Automatically look for .env files in common locations
                env_paths = [
                    Path.cwd() / ".env",  # Current working directory
                    Path(__file__).parent.parent.parent.parent / ".env",  # Project root
                    Path.home() / ".env",  # User home directory
                ]

                for env_path in env_paths:
                    if env_path.exists():
                        load_dotenv(env_path, override=True)
                        break

        # Create configuration directly from environment variables
        config = cls()

        # Re-initialize components to pick up any loaded env vars
        config.database = DatabaseConfig()
        config.api = APIConfig()
        config.processing = ProcessingConfig()
        config.parallelization = ParallelizationConfig()
        config.infrastructure = InfrastructureConfig()

        return config

    def enable_parallelization(
        self,
        congressional_sessions: int = 2,
        congressional_keys_per_session: int = 2,
        govinfo_sessions: int = 2,
        govinfo_keys_per_session: int = 2,
    ):
        """Enable parallelization with specified settings."""
        self.parallelization.enabled = True
        self.parallelization.fetcher = {
            "congressional": {
                "num_sessions": congressional_sessions,
                "keys_per_session": congressional_keys_per_session,
            },
            "govinfo": {
                "num_sessions": govinfo_sessions,
                "keys_per_session": govinfo_keys_per_session,
            },
        }

    def get_parallelization_config(self) -> dict[str, Any]:
        """Get parallelization configuration in expected format."""
        if not self.parallelization.enabled:
            return {}

        return {
            "fetcher": self.parallelization.fetcher,
            "normalizer": self.parallelization.normalizer,
            "cleaner": self.parallelization.cleaner,
        }

    def validate(self) -> list[str]:
        """Validate configuration and return list of errors."""
        errors = []

        # Validate API keys
        if not self.api.keys:
            errors.append("No API keys configured")

        # Validate database connection
        if not self.database.host:
            errors.append("Database host not configured")

        # Validate processing settings
        if self.processing.batch_size <= 0:
            errors.append("Batch size must be positive")

        if self.processing.max_workers <= 0:
            errors.append("Max workers must be positive")

        # Validate parallelization settings
        if self.parallelization.enabled and not self.parallelization.fetcher:
            errors.append("Parallelization enabled but no fetcher config provided")

        return errors

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "database": {
                "host": self.database.host,
                "port": self.database.port,
                "database": self.database.database,
                "username": self.database.username,
                # Don't include password in output
            },
            "api": {
                "key_count": len(self.api.keys),
                "rate_limit": self.api.rate_limit_per_second,
                "max_retries": self.api.max_retries,
                "timeout": self.api.timeout,
            },
            "processing": {
                "batch_size": self.processing.batch_size,
                "max_workers": self.processing.max_workers,
                "chunk_size": self.processing.chunk_size,
                "page_size": self.processing.page_size,
                "max_concurrent": self.processing.max_concurrent,
            },
            "parallelization": {
                "enabled": self.parallelization.enabled,
                "fetcher": self.parallelization.fetcher,
                "normalizer": self.parallelization.normalizer,
                "cleaner": self.parallelization.cleaner,
            },
            "infrastructure": {
                "checkpoint_db_path": self.infrastructure.checkpoint_db_path,
                "use_postgres_runs": self.infrastructure.use_postgres_runs,
                "use_postgres_checkpoints": self.infrastructure.use_postgres_checkpoints,
                "use_optimized_storage": self.infrastructure.use_optimized_storage,
                "use_dynamic_pool": self.infrastructure.use_dynamic_pool,
            },
        }
