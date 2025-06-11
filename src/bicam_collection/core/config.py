"""Configuration management for BICAM Collection."""

import os
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class DatabaseConfig(BaseSettings):
    """Database connection configuration."""

    host: str = Field(..., env="POSTGRESQL_HOST", description="PostgreSQL host")
    port: int = Field(5432, env="POSTGRESQL_PORT", description="PostgreSQL port")
    database: str = Field(..., env="POSTGRESQL_DATABASE", description="Database name")
    user: str = Field(..., env="POSTGRESQL_USER", description="Database user")
    password: str = Field(..., env="POSTGRESQL_PASSWORD", description="Database password")

    # Legacy environment variable support
    @field_validator("user", mode="before")
    @classmethod
    def user_fallback(cls, v):
        return v or os.getenv("POSTGRESQL_USERNAME")

    @field_validator("database", mode="before")
    @classmethod
    def database_fallback(cls, v):
        return v or os.getenv("POSTGRESQL_DB")

    @property
    def dsn(self) -> str:
        """Get the database DSN for asyncpg."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    class Config:
        env_prefix = "DB_"


class APIConfig(BaseSettings):
    """API configuration for external services."""

    congress_api_keys: list[str] = Field(
        default_factory=list,
        env="CONGRESS_API_KEYS",
        description="Congress.gov API keys (comma-separated)"
    )
    govinfo_api_key: str = Field(..., env="GOVINFO_API_KEY", description="GovInfo.gov API key")
    rate_limit: int = Field(22, env="API_RATE_LIMIT", description="API requests per second")
    max_retries: int = Field(3, env="API_MAX_RETRIES", description="Maximum API retry attempts")
    timeout: int = Field(30, env="API_TIMEOUT", description="API request timeout in seconds")

    @field_validator("congress_api_keys", mode="before")
    @classmethod
    def parse_api_keys(cls, v):
        """Parse comma-separated API keys or load from legacy env var."""
        if isinstance(v, str):
            keys = [key.strip() for key in v.split(",") if key.strip()]
            if keys:
                return keys

        # Fallback to legacy single key
        legacy_key = os.getenv("CONGRESS_API_KEY")
        if legacy_key:
            return [legacy_key]

        return v if isinstance(v, list) else []

    class Config:
        env_prefix = "API_"


class ProcessingConfig(BaseSettings):
    """Data processing configuration."""

    batch_size: int = Field(100, env="BATCH_SIZE", description="Processing batch size")
    max_workers: int = Field(10, env="MAX_WORKERS", description="Maximum worker processes")
    chunk_size: int = Field(1000, env="CHUNK_SIZE", description="Database chunk size")
    enable_parallel: bool = Field(True, env="ENABLE_PARALLEL", description="Enable parallel processing")

    class Config:
        env_prefix = "PROCESSING_"


class LoggingConfig(BaseSettings):
    """Logging configuration."""

    level: str = Field("INFO", env="LOG_LEVEL", description="Logging level")
    format: str = Field("json", env="LOG_FORMAT", description="Logging format (json|text)")
    file_path: str | None = Field(None, env="LOG_FILE", description="Log file path")

    class Config:
        env_prefix = "LOG_"


class Settings(BaseSettings):
    """Main application settings."""

    # Core configurations
    database: Optional[DatabaseConfig] = None
    api: Optional[APIConfig] = None
    processing: Optional[ProcessingConfig] = None
    logging: Optional[LoggingConfig] = None

    # Environment
    environment: str = Field("development", env="ENVIRONMENT", description="Application environment")
    debug: bool = Field(False, env="DEBUG", description="Enable debug mode")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    def __init__(self, **kwargs):
        """Initialize settings with support for multiple .env files."""
        super().__init__(**kwargs)

        # Load legacy .env files if they exist
        self._load_legacy_env_files()

        # Initialize sub-configurations
        try:
            self.database = DatabaseConfig()
        except Exception:
            self.database = None

        try:
            self.api = APIConfig()
        except Exception:
            self.api = None

        self.processing = ProcessingConfig()
        self.logging = LoggingConfig()

    def _load_legacy_env_files(self):
        """Load configuration from legacy .env files."""
        legacy_files = [".env.gov", ".env.info"]

        for file_path in legacy_files:
            if os.path.exists(file_path):
                from dotenv import load_dotenv
                load_dotenv(file_path)


# Global settings instance - will be None if configuration is invalid
try:
    settings = Settings()
except Exception:
    settings = None
