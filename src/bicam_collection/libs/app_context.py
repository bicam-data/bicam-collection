"""High-level application context shared across the whole bicam-collection pipeline.

The goal is to provide a *single* place where we wire together:
    • BicamConfig (all configuration – parsed once from env/yaml)
    • SystemAPIKeyManager (central API-key distribution)
    • CheckpointManager (SQLite checkpoints)
    • RunManager (either PostgreSQL or SQLite) – created lazily
    • A reusable asyncpg connection pool

Other layers (CLI runner, Dagster resources, fetchers, normalisers…) should *only*
receive either this context object or the specific component they need.  This removes
lots of duplicated boiler-plate currently scattered around `dagster_pipeline/main.py`
and the base classes.
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import asyncpg

from .api_key_manager import SystemAPIKeyManager
from .checkpoint import CheckpointManager
from .config import BicamConfig
from .run_tracking import RunManager

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ApplicationContext:
    """Aggregates long-lived singletons for a *single* pipeline run."""

    # Parsed configuration (immutable during one program run)
    config: BicamConfig

    # --- Infrastructure components (created lazily) -----------------------
    _api_key_manager: SystemAPIKeyManager | None = field(default=None, init=False)
    _checkpoint_manager: CheckpointManager | None = field(default=None, init=False)
    _run_manager: RunManager | None = field(default=None, init=False)
    _db_pool: asyncpg.Pool | None = field(default=None, init=False)

    # ------------------------------------------------------------------
    #  Dataclass lifecycle hooks
    # ------------------------------------------------------------------
    def __post_init__(self):
        """Apply logging configuration exactly once when the context is created."""
        self._configure_logging(self.config.logging)

    # ------------------------------------------------------------------
    #  Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _configure_logging(logging_cfg):  # type: ignore[override]
        """(Re)configure the root logger based on *LoggingConfig* settings.

        This centralises structured-logging setup so all components share the
        same configuration regardless of who imported *logging* first.
        """

        # Map level enum / string to numeric level
        level = (
            logging_cfg.level
            if isinstance(logging_cfg.level, int)
            else getattr(logging, str(logging_cfg.level).upper(), logging.INFO)
        )

        # Remove any previously registered root handlers to avoid duplicates
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # ------------------------------------------------------------------
        #  Build formatter – basic text vs. structured JSON
        # ------------------------------------------------------------------
        if logging_cfg.enable_structured_logging:

            class _JSONFormatter(logging.Formatter):
                def format(self, record: logging.LogRecord) -> str:  # noqa: D401
                    log_entry = {
                        "time": self.formatTime(record, self.datefmt),
                        "level": record.levelname,
                        "name": record.name,
                        "message": record.getMessage(),
                    }
                    if record.exc_info:
                        log_entry["exc_info"] = self.formatException(record.exc_info)
                    return json.dumps(log_entry, ensure_ascii=False)

            formatter: logging.Formatter = _JSONFormatter()
        else:
            formatter = logging.Formatter(logging_cfg.log_format)

        # ------------------------------------------------------------------
        #  Handlers – console and/or file
        # ------------------------------------------------------------------
        handlers: list[logging.Handler] = []

        if logging_cfg.log_to_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            handlers.append(console_handler)

        if logging_cfg.log_file:
            log_path = Path(logging_cfg.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)

        # Fallback to a default console handler if none configured
        if not handlers:
            fallback_handler = logging.StreamHandler(sys.stdout)
            fallback_handler.setFormatter(formatter)
            handlers.append(fallback_handler)

        root = logging.getLogger()
        root.setLevel(level)
        for h in handlers:
            root.addHandler(h)

        root.debug("Logging configured (level=%s, handlers=%s)", level, len(handlers))

    # ---------------------------------------------------------------------
    #  Public factory helpers
    # ---------------------------------------------------------------------
    def get_api_key_manager(self) -> SystemAPIKeyManager:
        if self._api_key_manager is None:
            keys: list[str] = []
            if self.config.scraping.congressional_api_key:
                keys.extend(
                    k.strip()
                    for k in self.config.scraping.congressional_api_key.split(",")
                    if k.strip()
                )
            if self.config.scraping.govinfo_api_key:
                keys.extend(
                    k.strip()
                    for k in self.config.scraping.govinfo_api_key.split(",")
                    if k.strip()
                )
            if not keys:
                raise RuntimeError("No API keys configured in BicamConfig")

            self._api_key_manager = SystemAPIKeyManager(api_keys=keys)
            logger.info("Created SystemAPIKeyManager with %s API keys", len(keys))
        return self._api_key_manager

    def get_checkpoint_manager(self):
        """Return checkpoint manager (SQLite or Postgres depending on config)."""
        if self._checkpoint_manager is None:
            if getattr(self.config.processing, "use_postgres_checkpoints", False):
                # Lazy import to avoid mandatory psycopg2 install for SQLite users
                from .pg_checkpoint import PostgresCheckpointManager

                db_cfg = self.config.database
                self._checkpoint_manager = PostgresCheckpointManager(
                    host=db_cfg.host,
                    port=db_cfg.port,
                    database=db_cfg.database,
                    user=db_cfg.username,
                    password=db_cfg.password,
                )
                logger.info("Using PostgreSQL-backed checkpoint manager")
            else:
                # Store SQLite checkpoints inside the project *data/checkpoints* dir
                project_root = (
                    Path(__file__).resolve().parents[4]
                )  # src/bicam_collection/libs/ → project root
                db_path = project_root / "data" / "checkpoints" / "checkpoints.db"
                self._checkpoint_manager = CheckpointManager(db_path)
                logger.info("Using SQLite checkpoint manager at %s", db_path)

        return self._checkpoint_manager

    async def get_db_pool(self) -> asyncpg.Pool:
        if self._db_pool is None:
            db_cfg = self.config.database
            self._db_pool = await asyncpg.create_pool(
                host=db_cfg.host,
                port=db_cfg.port,
                database=db_cfg.database,
                user=db_cfg.username,
                password=db_cfg.password,
                min_size=2,
                max_size=db_cfg.pool_size,
            )
            logger.info(
                "Created asyncpg pool to %s:%s/%s",
                db_cfg.host,
                db_cfg.port,
                db_cfg.database,
            )
        return self._db_pool

    async def get_run_manager(self) -> RunManager:
        if self._run_manager is None:
            # prefer PostgreSQL run tracking if we have a pool; else fallback to SQLite
            pool = await self.get_db_pool()
            self._run_manager = RunManager(use_postgres=True, external_pool=pool)
            await self._run_manager.initialize()
            logger.info("Initialised RunManager (postgres)")
        return self._run_manager

    # ------------------------------------------------------------------
    #  Cleanup helpers – call once at program shutdown
    # ------------------------------------------------------------------
    async def shutdown(self):
        """Gracefully close DB pool and other resources."""
        # Run manager first (commits remaining logs)
        if self._run_manager is not None:
            try:
                await self._run_manager.cleanup()
            except Exception as exc:  # pragma: no cover
                logger.warning("RunManager cleanup failed: %s", exc)
            self._run_manager = None

        # API key manager – just clear references so GC can reclaim
        self._api_key_manager = None

        # Checkpoint manager – no explicit close needed
        self._checkpoint_manager = None

        # DB pool last
        if self._db_pool is not None:
            try:
                await self._db_pool.close()
                logger.debug("asyncpg pool closed")
            except Exception as exc:  # pragma: no cover
                logger.warning("Error closing DB pool: %s", exc)
            self._db_pool = None

    # ------------------------------------------------------------------
    # Convenience summary ------------------------------------------------
    # ------------------------------------------------------------------
    def summary(self) -> dict[str, Any]:
        cfg = self.config
        return {
            "env": cfg.environment,
            "db": cfg.database.connection_string,
            "api_keys": len(self.get_api_key_manager().api_keys)
            if self._api_key_manager
            else "<lazy>",
        }
