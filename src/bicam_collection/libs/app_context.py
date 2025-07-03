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

import logging
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

    def get_checkpoint_manager(self) -> CheckpointManager:
        if self._checkpoint_manager is None:
            db_path = (
                Path(self.config.processing.temp_directory)
                / ".."
                / ".."
                / "checkpoints"
                / "checkpoints.db"
            ).resolve()
            self._checkpoint_manager = CheckpointManager(db_path)
            logger.info("Created CheckpointManager at %s", db_path)
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
