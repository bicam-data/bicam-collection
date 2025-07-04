"""
Bicam Collection Configuration System

This module provides comprehensive configuration management using Pydantic
for the bicam-collection data pipeline system.
"""

import logging
import os
from enum import Enum
from pathlib import Path
from urllib.parse import quote_plus

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings

# ------------------------------------------------------------------
#  Automatically load environment variables from a local *.env* file
#  -----------------------------------------------------------------
#  We defer the import so that *python-dotenv* only becomes a runtime
#  dependency when users rely on the implicit *.env* loading behaviour.
#  This keeps the library usable in restricted environments where the
#  package might be unavailable.
# ------------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore

    # Load *.env* from the current working directory (project root).  We do
    # *not* override pre-existing environment variables so explicit exports
    # still win.
    load_dotenv(override=False)
except ModuleNotFoundError:  # pragma: no cover – optional dependency
    # If python-dotenv is not installed the user must export variables
    # explicitly in the shell.  We don't hard-fail because most pipelines
    # are executed in container images where secrets are injected via
    # `docker run -e …` or Kubernetes ConfigMaps.
    pass

logger = logging.getLogger(__name__)


class LogLevel(str, Enum):
    """Logging levels"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class DatabaseConfig(BaseModel):
    """Database configuration settings"""

    host: str = Field(..., description="Database host")
    port: int = Field(5432, description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    pool_size: int = Field(10, description="Connection pool size")
    max_overflow: int = Field(20, description="Maximum connection overflow")
    pool_timeout: int = Field(30, description="Pool timeout in seconds")

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string"""
        username = quote_plus(self.username)
        password = quote_plus(self.password)
        return f"postgresql://{username}:{password}@{self.host}:{self.port}/{self.database}"

    @property
    def async_connection_string(self) -> str:
        """Generate async PostgreSQL connection string"""
        username = quote_plus(self.username)
        password = quote_plus(self.password)
        return f"postgresql+asyncpg://{username}:{password}@{self.host}:{self.port}/{self.database}"


class ScrapingConfig(BaseModel):
    """Scraping configuration settings"""

    congressional_api_key: str | None = Field(None, description="Congressional API key")
    govinfo_api_key: str | None = Field(None, description="GovInfo API key")
    max_concurrent_requests: int = Field(10, description="Maximum concurrent requests")
    request_timeout: int = Field(30, description="Request timeout in seconds")
    retry_attempts: int = Field(3, description="Number of retry attempts")
    retry_delay: float = Field(1.0, description="Delay between retries in seconds")
    rate_limit_delay: float = Field(
        0.1, description="Delay between requests in seconds"
    )
    output_directory: Path = Field(
        Path("/tmp/bicam-data"), description="Output directory for scraped data"
    )

    @field_validator("output_directory")
    def validate_output_directory(cls, v):
        """Ensure output directory exists or can be created"""
        if isinstance(v, str):
            v = Path(v)
        v.mkdir(parents=True, exist_ok=True)
        return v


class ProcessingConfig(BaseModel):
    """Data processing configuration settings"""

    chunk_size: int = Field(1000, description="Chunk size for batch processing")
    max_workers: int = Field(
        min(os.cpu_count() - 4, 32),
        description="Maximum number of worker processes",
    )
    temp_directory: Path = Field(
        Path("/tmp/bicam-processing"), description="Temporary processing directory"
    )
    memory_limit_gb: float = Field(4.0, description="Memory limit in GB")
    enable_parallel_processing: bool = Field(
        True, description="Enable parallel processing"
    )
    use_postgres_checkpoints: bool = Field(
        True,
        description="Store checkpoints in PostgreSQL instead of local SQLite",
    )

    @field_validator("temp_directory")
    def validate_temp_directory(cls, v):
        """Ensure temp directory exists or can be created"""
        if isinstance(v, str):
            v = Path(v)
        v.mkdir(parents=True, exist_ok=True)
        return v


class ExportConfig(BaseModel):
    """Export configuration settings"""

    export_directory: Path = Field(
        Path("/tmp/bicam-exports"), description="Export directory"
    )
    export_formats: list[str] = Field(["csv", "parquet"], description="Export formats")
    compress_exports: bool = Field(True, description="Compress exported files")
    s3_bucket: str | None = Field(None, description="S3 bucket for uploads")
    s3_prefix: str | None = Field(None, description="S3 prefix for uploads")

    @field_validator("export_directory")
    def validate_export_directory(cls, v):
        """Ensure export directory exists or can be created"""
        if isinstance(v, str):
            v = Path(v)
        v.mkdir(parents=True, exist_ok=True)
        return v

    @field_validator("export_formats")
    def validate_export_formats(cls, v):
        """Validate export formats"""
        valid_formats = {"csv", "parquet", "json", "xlsx"}
        for fmt in v:
            if fmt not in valid_formats:
                raise ValueError(
                    f"Invalid export format: {fmt}. Valid formats: {valid_formats}"
                )
        return v


class LobbyistMatchingConfig(BaseModel):
    """Lobbyist matching configuration settings"""

    similarity_threshold: float = Field(
        0.8, description="Similarity threshold for matching"
    )
    batch_size: int = Field(1000, description="Batch size for processing")
    timeout_seconds: int = Field(300, description="Timeout for processing in seconds")
    enable_fuzzy_matching: bool = Field(
        True, description="Enable fuzzy string matching"
    )

    @field_validator("similarity_threshold")
    def validate_similarity_threshold(cls, v):
        """Validate similarity threshold is between 0 and 1"""
        if not 0 <= v <= 1:
            raise ValueError("Similarity threshold must be between 0 and 1")
        return v


class LoggingConfig(BaseModel):
    """Logging configuration settings"""

    level: LogLevel = Field(LogLevel.INFO, description="Logging level")
    log_file: Path | None = Field(Path("logs/bicam.log"), description="Log file path")
    log_format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string",
    )
    enable_structured_logging: bool = Field(
        False,
        description="Enable structured logging (JSON lines).  Defaults to plain text logs.",
    )
    log_to_console: bool = Field(True, description="Log to console")


class BicamConfig(BaseSettings):
    """Main configuration class for Bicam Collection"""

    # Environment and general settings
    environment: str = Field(
        "development", description="Environment (development, staging, production)"
    )
    debug: bool = Field(False, description="Enable debug mode")

    # Component configurations
    database: DatabaseConfig = Field(..., description="Database configuration")
    scraping: ScrapingConfig = Field(
        default_factory=ScrapingConfig, description="Scraping configuration"
    )
    processing: ProcessingConfig = Field(
        default_factory=ProcessingConfig, description="Processing configuration"
    )
    export: ExportConfig = Field(
        default_factory=ExportConfig, description="Export configuration"
    )
    lobbyist_matching: LobbyistMatchingConfig = Field(
        default_factory=LobbyistMatchingConfig,
        description="Lobbyist matching configuration",
    )
    logging: LoggingConfig = Field(
        default_factory=LoggingConfig, description="Logging configuration"
    )

    # Pydantic v2 configuration (replaces the older nested `Config` class)
    model_config = {
        "extra": "allow",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "env_nested_delimiter": "__",
        # Map environment variables to nested config objects
        "env_mapping": {"database": "DATABASE"},
    }

    @classmethod
    def from_env(cls) -> "BicamConfig":
        """Create configuration from environment variables"""
        # Map environment variables to database config
        db_config = DatabaseConfig(
            host=os.getenv("POSTGRESQL_HOST", "localhost"),
            port=int(os.getenv("POSTGRESQL_PORT", "5432")),
            database=os.getenv("POSTGRESQL_DATABASE", "bicam"),
            username=os.getenv("POSTGRESQL_USERNAME", "postgres"),
            password=os.getenv("POSTGRESQL_PASSWORD", ""),
        )

        # ------------------------------------------------------------------
        #  Scraping API keys – support singular *…_API_KEY* or comma-separated
        #  *…_API_KEYS* variants.  If both are provided we merge them into a
        #  single comma-separated string so downstream splitting logic works.
        # ------------------------------------------------------------------

        def _merge_keys(single_var: str, multi_var: str) -> str | None:
            single = os.getenv(single_var)
            multi = os.getenv(multi_var)

            if single and multi:
                return ",".join([single, multi])
            return single or multi  # may be None

        scraping_config = ScrapingConfig(
            congressional_api_key=_merge_keys(
                "GOVINFO_API_KEYS", "CONGRESSIONAL_API_KEYS"
            ),
            govinfo_api_key=_merge_keys("GOVINFO_API_KEY", "GOVINFO_API_KEYS"),
            output_directory=Path(os.getenv("SCRAPING_OUTPUT_DIR", "/tmp/bicam-data")),
        )

        return cls(
            database=db_config,
            scraping=scraping_config,
            debug=os.getenv("DEBUG", "false").lower() == "true",
            environment=os.getenv("ENVIRONMENT", "development"),
        )

    @classmethod
    def from_yaml(cls, config_path: str | Path) -> "BicamConfig":
        """Load configuration from YAML file"""
        import yaml

        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path) as f:
            config_data = yaml.safe_load(f)

        return cls(**config_data)

    def save_to_yaml(self, config_path: str | Path) -> None:
        """Save configuration to YAML file"""
        import yaml

        config_path = Path(config_path)
        config_dict = self.dict()

        # Convert Path objects to strings for YAML serialization
        def convert_paths(obj):
            if isinstance(obj, dict):
                return {k: convert_paths(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_paths(item) for item in obj]
            elif isinstance(obj, Path):
                return str(obj)
            else:
                return obj

        config_dict = convert_paths(config_dict)

        with open(config_path, "w") as f:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)

    def validate_all_paths(self) -> bool:
        """Validate all path configurations"""
        try:
            # Validate that all directories can be created/accessed
            self.scraping.output_directory.mkdir(parents=True, exist_ok=True)
            self.processing.temp_directory.mkdir(parents=True, exist_ok=True)
            self.export.export_directory.mkdir(parents=True, exist_ok=True)

            # Validate configuration files exist (if provided - these are now optional)
            if (
                self.congressional_config_path
                and not self.congressional_config_path.exists()
            ):
                raise ValueError(
                    f"Congressional config not found: {self.congressional_config_path}"
                )
            if self.govinfo_config_path and not self.govinfo_config_path.exists():
                raise ValueError(
                    f"GovInfo config not found: {self.govinfo_config_path}"
                )

            return True
        except Exception as e:
            logger.error(f"Path validation error: {e}")
            return False


def load_config(config_path: str | Path | None = None) -> BicamConfig:
    """
    Load configuration from file or environment variables

    Args:
        config_path: Optional path to YAML configuration file

    Returns:
        BicamConfig instance
    """
    if config_path:
        return BicamConfig.from_yaml(config_path)
    else:
        return BicamConfig.from_env()


def create_sample_config(output_path: str | Path = "config.yaml") -> None:
    """Create a sample configuration file"""
    sample_config = BicamConfig.from_env()
    sample_config.save_to_yaml(output_path)
    logger.info(f"Sample configuration saved to: {output_path}")


if __name__ == "__main__":
    # Create sample configuration for testing
    create_sample_config()
