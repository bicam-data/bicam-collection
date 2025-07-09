"""
Hierarchical Checkpoint System for Multi-Stage Data Processing.

This module provides a comprehensive checkpointing solution that handles:
1. Fetching: List items → Full data → Related endpoints
2. Staging: JSONB → Normalized tables → Extracted lists
3. Cleaning: Staging tables → Production tables

Uses SQLite for fast checkpointing with optimized storage integration.
"""

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class ProcessingStage(str, Enum):
    """High-level processing stages."""

    FETCHING = "fetching"
    STAGING = "staging"
    CLEANING = "cleaning"


class FetchingPhase(str, Enum):
    """Fetching sub-phases."""

    LIST_ITEMS = "list_items"
    FULL_DATA = "full_data"
    RELATED_DATA = "related_data"
    FULL_RELATED_DATA = "full_related_data"


class StagingPhase(str, Enum):
    """Staging/normalization sub-phases."""

    JSONB_TO_STAGING = "jsonb_to_staging"
    EXTRACT_LISTS = "extract_lists"
    VALIDATE_STAGING = "validate_staging"


class CleaningPhase(str, Enum):
    """Cleaning sub-phases."""

    APPLY_RULES = "apply_rules"
    VALIDATE_PRODUCTION = "validate_production"


@dataclass
class CheckpointState:
    """Represents the current state of processing."""

    stage: ProcessingStage
    phase: str  # Phase within stage
    data_type: str

    # Position tracking
    current_item_id: str | None = None
    current_offset: int = 0
    current_table: str | None = None
    current_field: str | None = None
    current_endpoint: str | None = None

    # Progress tracking
    total_items: int = 0
    processed_items: int = 0
    failed_items: int = 0
    skipped_items: int = 0

    # Metadata
    started_at: datetime | None = None
    updated_at: datetime | None = None
    error_message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "stage": self.stage.value,
            "phase": self.phase,
            "data_type": self.data_type,
            "current_item_id": self.current_item_id,
            "current_offset": self.current_offset,
            "current_table": self.current_table,
            "current_field": self.current_field,
            "current_endpoint": self.current_endpoint,
            "total_items": self.total_items,
            "processed_items": self.processed_items,
            "failed_items": self.failed_items,
            "skipped_items": self.skipped_items,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "error_message": self.error_message,
            "metadata": json.dumps(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CheckpointState":
        """Create from dictionary."""
        return cls(
            stage=ProcessingStage(data["stage"]),
            phase=data["phase"],
            data_type=data["data_type"],
            current_item_id=data.get("current_item_id"),
            current_offset=data.get("current_offset", 0),
            current_table=data.get("current_table"),
            current_field=data.get("current_field"),
            current_endpoint=data.get("current_endpoint"),
            total_items=data.get("total_items", 0),
            processed_items=data.get("processed_items", 0),
            failed_items=data.get("failed_items", 0),
            skipped_items=data.get("skipped_items", 0),
            started_at=datetime.fromisoformat(data["started_at"])
            if data.get("started_at")
            else None,
            updated_at=datetime.fromisoformat(data["updated_at"])
            if data.get("updated_at")
            else None,
            error_message=data.get("error_message"),
            metadata=json.loads(data.get("metadata", "{}")),
        )


class HierarchicalCheckpointManager:
    """
    Manages hierarchical checkpoints across all processing stages.

    Features:
    - Lightweight SQLite storage for high-frequency updates
    - Hierarchical state tracking (stage → phase → item → field/endpoint)
    - Processed item tracking to avoid reprocessing
    - Error logging and retry management
    - Integration with optimized storage
    """

    def __init__(self, db_path: str = "hierarchical_checkpoints.db"):
        self.db_path = db_path
        self._init_db()

        # Cache for processed items (reduces DB queries)
        self._processed_cache: dict[str, set[str]] = {}
        self._cache_size = 10000

    def _init_db(self):
        """Initialize SQLite database with optimized settings."""
        conn = sqlite3.connect(self.db_path)

        # Optimize for write performance
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")

        # Main checkpoint table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stage TEXT NOT NULL,
                phase TEXT NOT NULL,
                data_type TEXT NOT NULL,
                current_item_id TEXT,
                current_offset INTEGER DEFAULT 0,
                current_table TEXT,
                current_field TEXT,
                current_endpoint TEXT,
                total_items INTEGER DEFAULT 0,
                processed_items INTEGER DEFAULT 0,
                failed_items INTEGER DEFAULT 0,
                skipped_items INTEGER DEFAULT 0,
                started_at TEXT,
                updated_at TEXT,
                error_message TEXT,
                metadata TEXT,
                UNIQUE(stage, phase, data_type)
            )
        """)

        # Processed items tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_items (
                stage TEXT NOT NULL,
                phase TEXT NOT NULL,
                data_type TEXT NOT NULL,
                item_id TEXT NOT NULL,
                sub_item TEXT,  -- For tracking related endpoints, fields, etc.
                processed_at REAL NOT NULL,
                PRIMARY KEY (stage, phase, data_type, item_id, sub_item)
            )
        """)

        # Create indexes for fast lookups
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_lookup
            ON processed_items(stage, phase, data_type, item_id)
        """)

        # Error tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processing_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stage TEXT NOT NULL,
                phase TEXT NOT NULL,
                data_type TEXT NOT NULL,
                item_id TEXT NOT NULL,
                sub_item TEXT,
                error_type TEXT,
                error_message TEXT,
                error_details TEXT,
                occurred_at REAL NOT NULL,
                retry_count INTEGER DEFAULT 0
            )
        """)

        conn.commit()
        conn.close()

    def get_or_create_checkpoint(
        self, stage: ProcessingStage, phase: str, data_type: str
    ) -> CheckpointState:
        """Get existing checkpoint or create new one."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            # Try to get existing
            cursor = conn.execute(
                """
                SELECT * FROM checkpoints
                WHERE stage = ? AND phase = ? AND data_type = ?
                """,
                (stage.value, phase, data_type),
            )
            row = cursor.fetchone()

            if row:
                return CheckpointState.from_dict(dict(row))

            # Create new checkpoint
            checkpoint = CheckpointState(
                stage=stage, phase=phase, data_type=data_type, started_at=datetime.now()
            )

            self.save_checkpoint(checkpoint)
            return checkpoint

        finally:
            conn.close()

    def save_checkpoint(self, checkpoint: CheckpointState):
        """Save checkpoint state (optimized for frequent updates)."""
        checkpoint.updated_at = datetime.now()

        conn = sqlite3.connect(self.db_path)
        try:
            data = checkpoint.to_dict()

            conn.execute(
                """
                INSERT OR REPLACE INTO checkpoints
                (stage, phase, data_type, current_item_id, current_offset,
                 current_table, current_field, current_endpoint,
                 total_items, processed_items, failed_items, skipped_items,
                 started_at, updated_at, error_message, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data["stage"],
                    data["phase"],
                    data["data_type"],
                    data["current_item_id"],
                    data["current_offset"],
                    data["current_table"],
                    data["current_field"],
                    data["current_endpoint"],
                    data["total_items"],
                    data["processed_items"],
                    data["failed_items"],
                    data["skipped_items"],
                    data["started_at"],
                    data["updated_at"],
                    data["error_message"],
                    data["metadata"],
                ),
            )
            conn.commit()

        finally:
            conn.close()

    def mark_item_processed(
        self,
        stage: ProcessingStage,
        phase: str,
        data_type: str,
        item_id: str,
        sub_item: str = "",
    ):
        """Mark an item as processed (uses cache for performance)."""
        cache_key = f"{stage.value}:{phase}:{data_type}"

        # Update cache
        if cache_key not in self._processed_cache:
            self._processed_cache[cache_key] = set()

        cache_item = f"{item_id}:{sub_item}" if sub_item else item_id
        self._processed_cache[cache_key].add(cache_item)

        # Flush to DB if cache is getting large
        if len(self._processed_cache[cache_key]) >= self._cache_size:
            self._flush_processed_cache(cache_key)

        # Also update DB immediately for checkpointing
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO processed_items
                (stage, phase, data_type, item_id, sub_item, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (stage.value, phase, data_type, item_id, sub_item, time.time()),
            )
            conn.commit()
        finally:
            conn.close()

    def is_item_processed(
        self,
        stage: ProcessingStage,
        phase: str,
        data_type: str,
        item_id: str,
        sub_item: str = "",
    ) -> bool:
        """Check if item is already processed (uses cache first)."""
        cache_key = f"{stage.value}:{phase}:{data_type}"
        cache_item = f"{item_id}:{sub_item}" if sub_item else item_id

        # Check cache first
        if (
            cache_key in self._processed_cache
            and cache_item in self._processed_cache[cache_key]
        ):
            return True

        # Check database
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute(
                """
                SELECT 1 FROM processed_items
                WHERE stage = ? AND phase = ? AND data_type = ?
                AND item_id = ? AND sub_item = ?
                LIMIT 1
                """,
                (stage.value, phase, data_type, item_id, sub_item),
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def log_error(
        self,
        stage: ProcessingStage,
        phase: str,
        data_type: str,
        item_id: str,
        error_message: str,
        error_type: str = "general",
        sub_item: str = "",
        error_details: str = "",
    ):
        """Log processing error."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                INSERT INTO processing_errors
                (stage, phase, data_type, item_id, sub_item,
                 error_type, error_message, error_details, occurred_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stage.value,
                    phase,
                    data_type,
                    item_id,
                    sub_item,
                    error_type,
                    error_message,
                    error_details,
                    time.time(),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_failed_items(
        self, stage: ProcessingStage, phase: str, data_type: str, max_retries: int = 3
    ) -> list[dict[str, Any]]:
        """Get failed items that haven't exceeded retry limit."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            cursor = conn.execute(
                """
                SELECT item_id, sub_item, error_type, error_message,
                        COUNT(*) as retry_count
                FROM processing_errors
                WHERE stage = ? AND phase = ? AND data_type = ?
                GROUP BY item_id, sub_item
                HAVING retry_count < ?
                """,
                (stage.value, phase, data_type, max_retries),
            )

            return [dict(row) for row in cursor.fetchall()]

        finally:
            conn.close()

    def _flush_processed_cache(self, cache_key: str):
        """Flush processed items cache to database."""
        if cache_key not in self._processed_cache:
            return

        items = list(self._processed_cache[cache_key])
        if not items:
            return

        stage, phase, data_type = cache_key.split(":")

        conn = sqlite3.connect(self.db_path)
        try:
            # Batch insert
            data = []
            for item in items:
                if ":" in item:
                    item_id, sub_item = item.split(":", 1)
                else:
                    item_id, sub_item = item, ""

                data.append((stage, phase, data_type, item_id, sub_item, time.time()))

            conn.executemany(
                """
                INSERT OR IGNORE INTO processed_items
                (stage, phase, data_type, item_id, sub_item, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                data,
            )
            conn.commit()

            # Clear cache
            self._processed_cache[cache_key].clear()

        finally:
            conn.close()

    def flush_all_caches(self):
        """Flush all cached data to database."""
        for cache_key in list(self._processed_cache.keys()):
            self._flush_processed_cache(cache_key)

    def get_progress_summary(self, data_type: str) -> dict[str, Any]:
        """Get progress summary across all stages for a data type."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            cursor = conn.execute(
                """
                SELECT stage, phase, processed_items, failed_items,
                        total_items, current_item_id, updated_at
                FROM checkpoints
                WHERE data_type = ?
                ORDER BY stage, phase
                """,
                (data_type,),
            )

            stages = {}
            for row in cursor.fetchall():
                stage = row["stage"]
                if stage not in stages:
                    stages[stage] = {}

                stages[stage][row["phase"]] = {
                    "processed": row["processed_items"],
                    "failed": row["failed_items"],
                    "total": row["total_items"],
                    "current_item": row["current_item_id"],
                    "last_updated": row["updated_at"],
                }

            return stages

        finally:
            conn.close()

    def reset_checkpoint(
        self,
        stage: ProcessingStage,
        phase: str,
        data_type: str,
        clear_processed: bool = True,
    ):
        """Reset a checkpoint to start fresh."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Delete checkpoint
            conn.execute(
                """
                DELETE FROM checkpoints
                WHERE stage = ? AND phase = ? AND data_type = ?
                """,
                (stage.value, phase, data_type),
            )

            if clear_processed:
                # Clear processed items
                conn.execute(
                    """
                    DELETE FROM processed_items
                    WHERE stage = ? AND phase = ? AND data_type = ?
                    """,
                    (stage.value, phase, data_type),
                )

                # Clear errors
                conn.execute(
                    """
                    DELETE FROM processing_errors
                    WHERE stage = ? AND phase = ? AND data_type = ?
                    """,
                    (stage.value, phase, data_type),
                )

            conn.commit()

            # Clear cache
            cache_key = f"{stage.value}:{phase}:{data_type}"
            if cache_key in self._processed_cache:
                del self._processed_cache[cache_key]

        finally:
            conn.close()


# Convenience classes for specific stages


class FetchingCheckpoint:
    """Specialized checkpoint manager for fetching stage."""

    def __init__(
        self, checkpoint_manager: HierarchicalCheckpointManager, data_type: str
    ):
        self.cm = checkpoint_manager
        self.data_type = data_type
        self.stage = ProcessingStage.FETCHING

    def mark_list_item_processed(self, item_id: str):
        """Mark a list item as processed."""
        self.cm.mark_item_processed(
            self.stage, FetchingPhase.LIST_ITEMS.value, self.data_type, item_id
        )

    def mark_full_data_processed(self, item_id: str):
        """Mark full data as fetched."""
        self.cm.mark_item_processed(
            self.stage, FetchingPhase.FULL_DATA.value, self.data_type, item_id
        )

    def mark_related_endpoint_processed(
        self,
        item_id: str,
        endpoint: str,
        data_source: str | None = None,
    ):
        """Mark a related endpoint as processed."""
        # Use consistent naming scheme
        if data_source == "govinfo":
            checkpoint_data_type = self.data_type
            sub_item = endpoint  # Use endpoint as sub_item for consistency
        else:
            checkpoint_data_type = self.data_type
            sub_item = endpoint  # Congressional uses endpoint as sub_item

        self.cm.mark_item_processed(
            self.stage,
            FetchingPhase.RELATED_DATA.value,
            checkpoint_data_type,
            item_id,
            sub_item=sub_item,
        )

    def should_skip_list_item(self, item_id: str) -> bool:
        """Check if list item was already processed."""
        return self.cm.is_item_processed(
            self.stage, FetchingPhase.LIST_ITEMS.value, self.data_type, item_id
        )

    def should_skip_full_data(self, item_id: str) -> bool:
        """Check if full data was already fetched."""
        return self.cm.is_item_processed(
            self.stage, FetchingPhase.FULL_DATA.value, self.data_type, item_id
        )

    def should_skip_related_endpoint(
        self,
        item_id: str,
        endpoint: str,
        data_source: str,
    ) -> bool:
        """Check if related endpoint was already processed."""
        # Use consistent naming scheme
        if data_source == "govinfo":
            checkpoint_data_type = self.data_type
            sub_item = endpoint  # Use endpoint as sub_item for consistency
        else:
            checkpoint_data_type = self.data_type
            sub_item = endpoint  # Congressional uses endpoint as sub_item

        return self.cm.is_item_processed(
            self.stage,
            FetchingPhase.RELATED_DATA.value,
            checkpoint_data_type,
            item_id,
            sub_item=sub_item,
        )

    def should_skip_full_related_data(self, item_id: str, data_source: str) -> bool:
        """Check if full related data was already fetched."""
        # Use consistent naming scheme
        if data_source == "govinfo":
            checkpoint_data_type = f"{self.data_type}_granules"
        else:
            checkpoint_data_type = self.data_type

        return self.cm.is_item_processed(
            self.stage,
            FetchingPhase.FULL_RELATED_DATA.value,
            checkpoint_data_type,
            item_id,
        )

    def get_checkpoint(self, phase: FetchingPhase) -> CheckpointState:
        """Get checkpoint for specific phase."""
        return self.cm.get_or_create_checkpoint(self.stage, phase.value, self.data_type)

    def save_checkpoint(self, checkpoint: CheckpointState):
        """Save checkpoint state."""
        self.cm.save_checkpoint(checkpoint)


class StagingCheckpoint:
    """Specialized checkpoint manager for staging/normalization."""

    def __init__(
        self, checkpoint_manager: HierarchicalCheckpointManager, data_type: str
    ):
        self.cm = checkpoint_manager
        self.data_type = data_type
        self.stage = ProcessingStage.STAGING

    def mark_table_processed(self, table_name: str):
        """Mark a table as normalized."""
        self.cm.mark_item_processed(
            self.stage, StagingPhase.JSONB_TO_STAGING.value, self.data_type, table_name
        )

    def mark_list_extracted(self, table_name: str, field_name: str):
        """Mark a list field as extracted to its own table."""
        self.cm.mark_item_processed(
            self.stage,
            StagingPhase.EXTRACT_LISTS.value,
            self.data_type,
            table_name,
            sub_item=field_name,
        )

    def should_skip_table(self, table_name: str) -> bool:
        """Check if table was already normalized."""
        return self.cm.is_item_processed(
            self.stage, StagingPhase.JSONB_TO_STAGING.value, self.data_type, table_name
        )

    def should_skip_list_extraction(self, table_name: str, field_name: str) -> bool:
        """Check if list was already extracted."""
        return self.cm.is_item_processed(
            self.stage,
            StagingPhase.EXTRACT_LISTS.value,
            self.data_type,
            table_name,
            sub_item=field_name,
        )

    def save_checkpoint(self, checkpoint: CheckpointState):
        """Save checkpoint state."""
        self.cm.save_checkpoint(checkpoint)


class CleaningCheckpoint:
    """Specialized checkpoint manager for cleaning stage."""

    def __init__(
        self, checkpoint_manager: HierarchicalCheckpointManager, data_type: str
    ):
        self.cm = checkpoint_manager
        self.data_type = data_type
        self.stage = ProcessingStage.CLEANING

    def mark_table_cleaned(self, table_name: str):
        """Mark a table as cleaned and moved to production."""
        self.cm.mark_item_processed(
            self.stage, CleaningPhase.APPLY_RULES.value, self.data_type, table_name
        )

    def should_skip_table(self, table_name: str) -> bool:
        """Check if table was already cleaned."""
        return self.cm.is_item_processed(
            self.stage, CleaningPhase.APPLY_RULES.value, self.data_type, table_name
        )

    def save_checkpoint(self, checkpoint: CheckpointState):
        """Save checkpoint state."""
        self.cm.save_checkpoint(checkpoint)
