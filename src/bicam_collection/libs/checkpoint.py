"""
Checkpoint Management System

This module provides checkpoint functionality for resuming scraping
operations from where they left off, with support for hierarchical
data processing including nested fields and related entities.
"""

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class CheckpointStatus(str, Enum):
    """Status of a checkpoint"""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ProcessingPhase(str, Enum):
    """Different phases of data processing"""

    MAIN_ITEMS = "main_items"
    NESTED_FIELDS = "nested_fields"
    RELATED_ENTITIES = "related_entities"
    SECONDARY_RELATIONS = "secondary_relations"


class ProcessingStage(str, Enum):
    """High-level processing stages for data pipeline"""

    SCRAPING = "scraping"  # Fetching raw data from APIs
    NORMALIZATION = "normalization"  # Processing raw data to staging schema
    CLEANING = "cleaning"  # Processing staging data to production schema
    ANALYSIS = "analysis"  # Any post-processing analysis


@dataclass
class ProcessingState:
    """Detailed processing state for hierarchical data processing"""

    phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS
    item_index: int = 0
    item_total: int = 0
    field_index: int = 0
    field_total: int = 0
    relation_index: int = 0
    relation_total: int = 0
    secondary_index: int = 0
    secondary_total: int = 0
    current_field_name: str = ""
    current_relation_type: str = ""
    metadata: dict = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class CheckpointData:
    """Enhanced data stored in a checkpoint with hierarchical processing support"""

    scraper_type: str  # 'congressional' or 'govinfo'
    data_type: str  # 'bills', 'amendments', etc.
    last_processed_id: str | None = None
    last_processed_date: str | None = None
    total_items: int = 0
    processed_items: int = 0
    failed_items: int = 0
    status: CheckpointStatus = CheckpointStatus.PENDING
    created_at: str = ""
    updated_at: str = ""

    # Enhanced hierarchical processing fields
    processing_state: ProcessingState | None = None
    api_params: dict = None  # Store API parameters for resuming
    last_exported_item: dict = None  # Last successfully exported item
    batch_info: dict = None  # Current batch information

    # Legacy metadata for backward compatibility
    metadata: dict = None

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        self.updated_at = datetime.now().isoformat()
        if self.metadata is None:
            self.metadata = {}
        if self.api_params is None:
            self.api_params = {}
        if self.batch_info is None:
            self.batch_info = {}
        if self.processing_state is None:
            self.processing_state = ProcessingState()


class CheckpointManager:
    """Enhanced manager for scraping checkpoints with hierarchical processing support"""

    def __init__(self, db_path: Path | str = Path("checkpoints.db")):
        import os

        self.db_path = Path(db_path)

        # Ensure we have an absolute path
        if not self.db_path.is_absolute():
            # Convert to absolute path based on current working directory
            self.db_path = Path.cwd() / self.db_path

        logger.info(f"CheckpointManager initializing with path: {self.db_path}")
        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"Path exists: {self.db_path.exists()}")
        logger.info(f"Parent exists: {self.db_path.parent.exists()}")

        # Create parent directory
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Test file accessibility before proceeding
        try:
            # Try to touch the file to ensure we can write to it
            if not self.db_path.exists():
                self.db_path.touch()
            # Test basic SQLite connection
            import sqlite3

            test_conn = sqlite3.connect(str(self.db_path))
            test_conn.close()
            logger.info(f"SQLite connectivity test passed: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to access database file {self.db_path}: {e}")
            logger.error(f"Parent directory: {self.db_path.parent}")
            logger.error(f"Parent exists: {self.db_path.parent.exists()}")
            logger.error(
                f"Parent permissions: {oct(self.db_path.parent.stat().st_mode)[-3:] if self.db_path.parent.exists() else 'N/A'}"
            )
            raise

        self._init_database()

    def _get_connection(self):
        """Get a SQLite connection with settings optimized for concurrent access."""
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,  # 30 second timeout for concurrent access
            check_same_thread=False,  # Allow access from multiple threads
        )
        # Enable WAL mode for better concurrent access
        conn.execute("PRAGMA journal_mode=WAL")
        # Set busy timeout for concurrent access
        conn.execute("PRAGMA busy_timeout=30000")  # 30 seconds
        return conn

    def _init_database(self):
        """Initialize the enhanced checkpoint database"""
        try:
            with self._get_connection() as conn:
                # Enable immediate durability for initialization
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        scraper_type TEXT NOT NULL,
                        data_type TEXT NOT NULL,
                        last_processed_id TEXT,
                        last_processed_date TEXT,
                        total_items INTEGER DEFAULT 0,
                        processed_items INTEGER DEFAULT 0,
                        failed_items INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'pending',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,

                        -- Enhanced hierarchical processing fields
                        processing_state TEXT DEFAULT '{}',
                        api_params TEXT DEFAULT '{}',
                        last_exported_item TEXT DEFAULT '{}',
                        batch_info TEXT DEFAULT '{}',

                        -- Legacy metadata for backward compatibility
                        metadata TEXT DEFAULT '{}',

                        UNIQUE(scraper_type, data_type)
                    )
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS checkpoint_errors (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        checkpoint_id INTEGER,
                        item_id TEXT,
                        error_message TEXT,
                        error_details TEXT,
                        processing_phase TEXT,
                        field_name TEXT,
                        relation_type TEXT,
                        created_at TEXT NOT NULL,
                        FOREIGN KEY(checkpoint_id) REFERENCES checkpoints(id)
                    )
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS processed_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        checkpoint_id INTEGER,
                        item_id TEXT NOT NULL,
                        processing_phase TEXT NOT NULL,
                        field_name TEXT,
                        relation_type TEXT,
                        processed_at TEXT NOT NULL,
                        FOREIGN KEY(checkpoint_id) REFERENCES checkpoints(id),
                        UNIQUE(checkpoint_id, item_id, processing_phase, field_name, relation_type)
                    )
                """)

                # Add processing phases table for tracking detailed progress
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS processing_phases (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        checkpoint_id INTEGER,
                        phase TEXT NOT NULL,
                        item_index INTEGER DEFAULT 0,
                        total_items INTEGER DEFAULT 0,
                        field_index INTEGER DEFAULT 0,
                        total_fields INTEGER DEFAULT 0,
                        relation_index INTEGER DEFAULT 0,
                        total_relations INTEGER DEFAULT 0,
                        current_field_name TEXT,
                        current_relation_type TEXT,
                        phase_metadata TEXT DEFAULT '{}',
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY(checkpoint_id) REFERENCES checkpoints(id),
                        UNIQUE(checkpoint_id, phase)
                    )
                """)
        except sqlite3.Error as e:
            logger.error(f"SQLite error during database initialization: {e}")
            logger.error(f"Database path: {self.db_path}")
            logger.error(f"Path exists: {self.db_path.exists()}")
            logger.error(f"Path is file: {self.db_path.is_file()}")
            logger.error(f"Parent directory: {self.db_path.parent}")
            logger.error(f"Parent exists: {self.db_path.parent.exists()}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during database initialization: {e}")
            logger.error(f"Database path: {self.db_path}")
            raise

    def save_checkpoint(self, checkpoint: CheckpointData) -> int:
        """Save or update a checkpoint with hierarchical processing state"""
        checkpoint.updated_at = datetime.now().isoformat()

        with self._get_connection() as conn:
            # Ensure immediate durability for checkpoints
            conn.execute("PRAGMA synchronous = FULL")
            # Serialize complex objects
            processing_state_json = json.dumps(
                {
                    "phase": checkpoint.processing_state.phase.value,
                    "item_index": checkpoint.processing_state.item_index,
                    "item_total": checkpoint.processing_state.item_total,
                    "field_index": checkpoint.processing_state.field_index,
                    "field_total": checkpoint.processing_state.field_total,
                    "relation_index": checkpoint.processing_state.relation_index,
                    "relation_total": checkpoint.processing_state.relation_total,
                    "secondary_index": checkpoint.processing_state.secondary_index,
                    "secondary_total": checkpoint.processing_state.secondary_total,
                    "current_field_name": checkpoint.processing_state.current_field_name,
                    "current_relation_type": checkpoint.processing_state.current_relation_type,
                    "metadata": checkpoint.processing_state.metadata,
                }
            )

            # First check if checkpoint already exists
            existing_checkpoint = conn.execute(
                "SELECT id FROM checkpoints WHERE scraper_type = ? AND data_type = ?",
                (checkpoint.scraper_type, checkpoint.data_type),
            ).fetchone()

            if existing_checkpoint:
                # Update existing checkpoint
                checkpoint_id = existing_checkpoint[0]
                logger.debug(f"Updating existing checkpoint ID: {checkpoint_id}")
                conn.execute(
                    """
                    UPDATE checkpoints SET
                        last_processed_id = ?, last_processed_date = ?,
                        total_items = ?, processed_items = ?, failed_items = ?, status = ?,
                        updated_at = ?, processing_state = ?, api_params = ?,
                        last_exported_item = ?, batch_info = ?, metadata = ?
                    WHERE id = ?
                """,
                    (
                        checkpoint.last_processed_id,
                        checkpoint.last_processed_date,
                        checkpoint.total_items,
                        checkpoint.processed_items,
                        checkpoint.failed_items,
                        checkpoint.status.value,
                        checkpoint.updated_at,
                        processing_state_json,
                        json.dumps(checkpoint.api_params),
                        json.dumps(checkpoint.last_exported_item or {}),
                        json.dumps(checkpoint.batch_info),
                        json.dumps(checkpoint.metadata),
                        checkpoint_id,
                    ),
                )
            else:
                # Insert new checkpoint
                logger.debug("Creating new checkpoint")
                cursor = conn.execute(
                    """
                    INSERT INTO checkpoints
                    (scraper_type, data_type, last_processed_id, last_processed_date,
                    total_items, processed_items, failed_items, status,
                    created_at, updated_at, processing_state, api_params,
                    last_exported_item, batch_info, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        checkpoint.scraper_type,
                        checkpoint.data_type,
                        checkpoint.last_processed_id,
                        checkpoint.last_processed_date,
                        checkpoint.total_items,
                        checkpoint.processed_items,
                        checkpoint.failed_items,
                        checkpoint.status.value,
                        checkpoint.created_at,
                        checkpoint.updated_at,
                        processing_state_json,
                        json.dumps(checkpoint.api_params),
                        json.dumps(checkpoint.last_exported_item or {}),
                        json.dumps(checkpoint.batch_info),
                        json.dumps(checkpoint.metadata),
                    ),
                )
                checkpoint_id = cursor.lastrowid

            # Explicitly commit to ensure the transaction is persisted
            conn.commit()

            logger.debug(
                f"Saved checkpoint for {checkpoint.scraper_type}:{checkpoint.data_type}"
            )
            return checkpoint_id

    def load_checkpoint(
        self, scraper_type: str, data_type: str
    ) -> CheckpointData | None:
        """Load a checkpoint with hierarchical processing state"""
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            result = conn.execute(
                """
                SELECT * FROM checkpoints
                WHERE scraper_type = ? AND data_type = ?
            """,
                (scraper_type, data_type),
            ).fetchone()

            if not result:
                return None

            # Deserialize processing state
            processing_state_data = json.loads(result["processing_state"] or "{}")
            processing_state = ProcessingState(
                phase=ProcessingPhase(
                    processing_state_data.get("phase", ProcessingPhase.MAIN_ITEMS.value)
                ),
                item_index=processing_state_data.get("item_index", 0),
                item_total=processing_state_data.get("item_total", 0),
                field_index=processing_state_data.get("field_index", 0),
                field_total=processing_state_data.get("field_total", 0),
                relation_index=processing_state_data.get("relation_index", 0),
                relation_total=processing_state_data.get("relation_total", 0),
                secondary_index=processing_state_data.get("secondary_index", 0),
                secondary_total=processing_state_data.get("secondary_total", 0),
                current_field_name=processing_state_data.get("current_field_name", ""),
                current_relation_type=processing_state_data.get(
                    "current_relation_type", ""
                ),
                metadata=processing_state_data.get("metadata", {}),
            )

            return CheckpointData(
                scraper_type=result["scraper_type"],
                data_type=result["data_type"],
                last_processed_id=result["last_processed_id"],
                last_processed_date=result["last_processed_date"],
                total_items=result["total_items"],
                processed_items=result["processed_items"],
                failed_items=result["failed_items"],
                status=CheckpointStatus(result["status"]),
                created_at=result["created_at"],
                updated_at=result["updated_at"],
                processing_state=processing_state,
                api_params=json.loads(result["api_params"] or "{}"),
                last_exported_item=json.loads(result["last_exported_item"] or "{}"),
                batch_info=json.loads(result["batch_info"] or "{}"),
                metadata=json.loads(result["metadata"] or "{}"),
            )

    def list_checkpoints(self, scraper_type: str | None = None) -> list[CheckpointData]:
        """List all checkpoints, optionally filtered by scraper type"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            if scraper_type:
                results = conn.execute(
                    "SELECT * FROM checkpoints WHERE scraper_type = ? ORDER BY updated_at DESC",
                    (scraper_type,),
                ).fetchall()
            else:
                results = conn.execute(
                    "SELECT * FROM checkpoints ORDER BY updated_at DESC"
                ).fetchall()

            checkpoints = []
            for result in results:
                # Deserialize processing state
                processing_state_data = json.loads(result["processing_state"] or "{}")
                processing_state = ProcessingState(
                    phase=ProcessingPhase(
                        processing_state_data.get(
                            "phase", ProcessingPhase.MAIN_ITEMS.value
                        )
                    ),
                    item_index=processing_state_data.get("item_index", 0),
                    item_total=processing_state_data.get("item_total", 0),
                    field_index=processing_state_data.get("field_index", 0),
                    field_total=processing_state_data.get("field_total", 0),
                    relation_index=processing_state_data.get("relation_index", 0),
                    relation_total=processing_state_data.get("relation_total", 0),
                    secondary_index=processing_state_data.get("secondary_index", 0),
                    secondary_total=processing_state_data.get("secondary_total", 0),
                    current_field_name=processing_state_data.get(
                        "current_field_name", ""
                    ),
                    current_relation_type=processing_state_data.get(
                        "current_relation_type", ""
                    ),
                    metadata=processing_state_data.get("metadata", {}),
                )

                checkpoints.append(
                    CheckpointData(
                        scraper_type=result["scraper_type"],
                        data_type=result["data_type"],
                        last_processed_id=result["last_processed_id"],
                        last_processed_date=result["last_processed_date"],
                        total_items=result["total_items"],
                        processed_items=result["processed_items"],
                        failed_items=result["failed_items"],
                        status=CheckpointStatus(result["status"]),
                        created_at=result["created_at"],
                        updated_at=result["updated_at"],
                        processing_state=processing_state,
                        api_params=json.loads(result["api_params"] or "{}"),
                        last_exported_item=json.loads(
                            result["last_exported_item"] or "{}"
                        ),
                        batch_info=json.loads(result["batch_info"] or "{}"),
                        metadata=json.loads(result["metadata"] or "{}"),
                    )
                )

            return checkpoints

    def mark_item_processed(
        self,
        scraper_type: str,
        data_type: str,
        item_id: str,
        phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS,
        field_name: str = "",
        relation_type: str = "",
    ) -> None:
        """Mark an individual item as processed in a specific phase"""
        checkpoint_id = self._get_checkpoint_id(scraper_type, data_type)
        if not checkpoint_id:
            logger.warning(
                f"No checkpoint ID found for {scraper_type}:{data_type} when marking {item_id}"
            )
            return

        try:
            with sqlite3.connect(self.db_path) as conn:
                # Ensure immediate durability for checkpoints
                conn.execute("PRAGMA synchronous = FULL")

                # Log what we're about to insert
                logger.debug(
                    f"Marking item processed: checkpoint_id={checkpoint_id}, item_id={item_id}, phase={phase.value}, field_name='{field_name}', relation_type='{relation_type}'"
                )

                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO processed_items
                    (checkpoint_id, item_id, processing_phase, field_name, relation_type, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        checkpoint_id,
                        item_id,
                        phase.value,
                        field_name,
                        relation_type,
                        datetime.now().isoformat(),
                    ),
                )

                # Check if a row was actually inserted
                rows_affected = cursor.rowcount
                logger.debug(f"INSERT operation affected {rows_affected} rows")

                # Explicitly commit to ensure the transaction is persisted
                conn.commit()

                # Verify the insertion worked
                result = conn.execute(
                    """
                    SELECT 1 FROM processed_items
                    WHERE checkpoint_id = ? AND item_id = ? AND processing_phase = ?
                    AND field_name = ? AND relation_type = ?
                """,
                    (checkpoint_id, item_id, phase.value, field_name, relation_type),
                ).fetchone()

                if result:
                    logger.debug(
                        f"✅ Successfully marked {item_id} as processed in phase {phase.value}"
                    )
                else:
                    logger.error(
                        f"❌ Failed to mark {item_id} as processed - verification query returned None"
                    )

                    # Show what records do exist for debugging
                    existing = conn.execute(
                        "SELECT item_id, processing_phase FROM processed_items WHERE checkpoint_id = ? LIMIT 5",
                        (checkpoint_id,),
                    ).fetchall()
                    logger.error(
                        f"Sample existing records for checkpoint {checkpoint_id}: {existing}"
                    )

        except sqlite3.Error as e:
            logger.error(f"SQLite error marking {item_id} as processed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error marking {item_id} as processed: {e}")

    def is_item_processed(
        self,
        scraper_type: str,
        data_type: str,
        item_id: str,
        phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS,
        field_name: str = "",
        relation_type: str = "",
    ) -> bool:
        """Check if an item has been processed in a specific phase"""
        checkpoint_id = self._get_checkpoint_id(scraper_type, data_type)
        if not checkpoint_id:
            logger.debug(
                f"No checkpoint ID found for {scraper_type}:{data_type} when checking {item_id}"
            )
            return False

        with sqlite3.connect(self.db_path) as conn:
            logger.debug(
                f"Checking if item processed: checkpoint_id={checkpoint_id}, item_id={item_id}, phase={phase.value}, field_name='{field_name}', relation_type='{relation_type}'"
            )

            result = conn.execute(
                """
                SELECT 1 FROM processed_items
                WHERE checkpoint_id = ? AND item_id = ? AND processing_phase = ?
                AND field_name = ? AND relation_type = ?
            """,
                (checkpoint_id, item_id, phase.value, field_name, relation_type),
            ).fetchone()

            is_processed = result is not None
            logger.debug(f"Item {item_id} processed status: {is_processed}")

            # Also show all processed items for this checkpoint for debugging
            if not is_processed:
                all_processed = conn.execute(
                    """
                    SELECT item_id, processing_phase, field_name, relation_type
                    FROM processed_items
                    WHERE checkpoint_id = ?
                    ORDER BY processed_at DESC
                    LIMIT 10
                """,
                    (checkpoint_id,),
                ).fetchall()

                logger.debug(
                    f"Recent processed items for this checkpoint: {all_processed}"
                )

            return is_processed

    def log_error(
        self,
        scraper_type: str,
        data_type: str,
        item_id: str,
        error_message: str,
        error_details: str = "",
        phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS,
        field_name: str = "",
        relation_type: str = "",
    ) -> None:
        """Log an error for a specific item in a specific processing phase"""
        checkpoint_id = self._get_checkpoint_id(scraper_type, data_type)
        if not checkpoint_id:
            return

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO checkpoint_errors
                (checkpoint_id, item_id, error_message, error_details,
                processing_phase, field_name, relation_type, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    checkpoint_id,
                    item_id,
                    error_message,
                    error_details,
                    phase.value,
                    field_name,
                    relation_type,
                    datetime.now().isoformat(),
                ),
            )

    def get_errors(self, scraper_type: str, data_type: str) -> list[dict[str, Any]]:
        """Get all errors for a checkpoint"""
        checkpoint_id = self._get_checkpoint_id(scraper_type, data_type)
        if not checkpoint_id:
            return []

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            results = conn.execute(
                """
                SELECT item_id, error_message, error_details, processing_phase,
                        field_name, relation_type, created_at
                FROM checkpoint_errors
                WHERE checkpoint_id = ?
                ORDER BY created_at DESC
            """,
                (checkpoint_id,),
            ).fetchall()

            return [dict(row) for row in results]

    def reset_checkpoint(self, scraper_type: str, data_type: str) -> None:
        """Reset a checkpoint to start fresh"""
        checkpoint_id = self._get_checkpoint_id(scraper_type, data_type)
        if not checkpoint_id:
            return

        with sqlite3.connect(self.db_path) as conn:
            # Clear processed items
            conn.execute(
                "DELETE FROM processed_items WHERE checkpoint_id = ?", (checkpoint_id,)
            )

            # Clear errors
            conn.execute(
                "DELETE FROM checkpoint_errors WHERE checkpoint_id = ?",
                (checkpoint_id,),
            )

            # Clear processing phases
            conn.execute(
                "DELETE FROM processing_phases WHERE checkpoint_id = ?",
                (checkpoint_id,),
            )

            # Reset checkpoint with fresh processing state
            fresh_state = ProcessingState()
            processing_state_json = json.dumps(
                {
                    "phase": fresh_state.phase.value,
                    "item_index": 0,
                    "item_total": 0,
                    "field_index": 0,
                    "field_total": 0,
                    "relation_index": 0,
                    "relation_total": 0,
                    "secondary_index": 0,
                    "secondary_total": 0,
                    "current_field_name": "",
                    "current_relation_type": "",
                    "metadata": {},
                }
            )

            conn.execute(
                """
                UPDATE checkpoints
                SET last_processed_id = NULL, last_processed_date = NULL,
                    processed_items = 0, failed_items = 0, status = 'pending',
                    processing_state = ?, api_params = '{}',
                    last_exported_item = '{}', batch_info = '{}',
                    updated_at = ?
                WHERE id = ?
            """,
                (processing_state_json, datetime.now().isoformat(), checkpoint_id),
            )

        logger.info(f"Reset checkpoint for {scraper_type}:{data_type}")

    def delete_checkpoint(self, scraper_type: str, data_type: str) -> None:
        """Delete a checkpoint and all associated data"""
        checkpoint_id = self._get_checkpoint_id(scraper_type, data_type)
        if not checkpoint_id:
            return

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM processed_items WHERE checkpoint_id = ?", (checkpoint_id,)
            )
            conn.execute(
                "DELETE FROM checkpoint_errors WHERE checkpoint_id = ?",
                (checkpoint_id,),
            )
            conn.execute(
                "DELETE FROM processing_phases WHERE checkpoint_id = ?",
                (checkpoint_id,),
            )
            conn.execute("DELETE FROM checkpoints WHERE id = ?", (checkpoint_id,))

        logger.info(f"Deleted checkpoint for {scraper_type}:{data_type}")

    def get_progress_summary(self, scraper_type: str | None = None) -> dict[str, Any]:
        """Get a summary of progress across all checkpoints"""
        with sqlite3.connect(self.db_path) as conn:
            if scraper_type:
                query = """
                    SELECT
                        COUNT(*) as total_checkpoints,
                        SUM(total_items) as total_items,
                        SUM(processed_items) as processed_items,
                        SUM(failed_items) as failed_items,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_checkpoints
                    FROM checkpoints
                    WHERE scraper_type = ?
                """
                result = conn.execute(query, (scraper_type,)).fetchone()
            else:
                query = """
                    SELECT
                        COUNT(*) as total_checkpoints,
                        SUM(total_items) as total_items,
                        SUM(processed_items) as processed_items,
                        SUM(failed_items) as failed_items,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_checkpoints
                    FROM checkpoints
                """
                result = conn.execute(query).fetchone()

            if result and result[0] > 0:
                total_items = result[1] or 0
                processed_items = result[2] or 0
                progress_percent = (
                    (processed_items / total_items * 100) if total_items > 0 else 0
                )

                return {
                    "total_checkpoints": result[0],
                    "completed_checkpoints": result[4],
                    "total_items": total_items,
                    "processed_items": processed_items,
                    "failed_items": result[3] or 0,
                    "progress_percent": round(progress_percent, 2),
                }
            else:
                return {
                    "total_checkpoints": 0,
                    "completed_checkpoints": 0,
                    "total_items": 0,
                    "processed_items": 0,
                    "failed_items": 0,
                    "progress_percent": 0.0,
                }

    def _get_checkpoint_id(self, scraper_type: str, data_type: str) -> int | None:
        """Get checkpoint ID by scraper and data type"""
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute(
                "SELECT id FROM checkpoints WHERE scraper_type = ? AND data_type = ?",
                (scraper_type, data_type),
            ).fetchone()

            checkpoint_id = result[0] if result else None
            logger.debug(
                f"_get_checkpoint_id({scraper_type}, {data_type}) = {checkpoint_id}"
            )
            return checkpoint_id


class HierarchicalProgressTracker:
    """Enhanced progress tracker for hierarchical data processing"""

    def __init__(
        self, checkpoint_manager: CheckpointManager, scraper_type: str, data_type: str
    ):
        self.checkpoint_manager = checkpoint_manager
        self.scraper_type = scraper_type
        self.data_type = data_type
        self.checkpoint = self.checkpoint_manager.load_checkpoint(
            scraper_type, data_type
        )

        if not self.checkpoint:
            self.checkpoint = CheckpointData(
                scraper_type=scraper_type,
                data_type=data_type,
                status=CheckpointStatus.PENDING,
                processing_state=ProcessingState(),
            )
            self.checkpoint_manager.save_checkpoint(self.checkpoint)

    def start_processing(self, total_items: int = 0):
        """Mark processing as started"""
        self.checkpoint.status = CheckpointStatus.IN_PROGRESS
        if total_items > 0:
            self.checkpoint.total_items = total_items
            self.checkpoint.processing_state.item_total = total_items
        self.checkpoint_manager.save_checkpoint(self.checkpoint)

        logger.info(f"Started processing {self.scraper_type}:{self.data_type}")

    def set_processing_phase(
        self,
        phase: ProcessingPhase,
        item_index: int = 0,
        item_total: int = 0,
        field_index: int = 0,
        field_total: int = 0,
        relation_index: int = 0,
        relation_total: int = 0,
        current_field: str = "",
        current_relation: str = "",
    ):
        """Set the current processing phase with detailed position"""
        self.checkpoint.processing_state.phase = phase
        self.checkpoint.processing_state.item_index = item_index
        self.checkpoint.processing_state.item_total = item_total
        self.checkpoint.processing_state.field_index = field_index
        self.checkpoint.processing_state.field_total = field_total
        self.checkpoint.processing_state.relation_index = relation_index
        self.checkpoint.processing_state.relation_total = relation_total
        self.checkpoint.processing_state.current_field_name = current_field
        self.checkpoint.processing_state.current_relation_type = current_relation

        self.checkpoint_manager.save_checkpoint(self.checkpoint)

        logger.debug(
            f"Set phase {phase.value} for {self.data_type}: "
            f"item {item_index}/{item_total}, field {field_index}/{field_total}, "
            f"relation {relation_index}/{relation_total}"
        )

    def update_progress(
        self,
        processed_items: int | None = None,
        last_processed_id: str | None = None,
        last_exported_item: dict | None = None,
        api_params: dict | None = None,
        last_processed_date: str | None = None,
    ):
        """Update progress information. Extra kwargs are tolerated for backward compatibility."""
        if processed_items is not None:
            self.checkpoint.processed_items = processed_items
        if last_processed_id is not None:
            self.checkpoint.last_processed_id = last_processed_id
        if last_exported_item is not None:
            self.checkpoint.last_exported_item = last_exported_item
        if api_params is not None:
            self.checkpoint.api_params = api_params
        if last_processed_date is not None:
            self.checkpoint.last_processed_date = last_processed_date

        # Ignore any other unexpected kwargs to keep API flexible

        self.checkpoint_manager.save_checkpoint(self.checkpoint)

    def increment_processed(
        self,
        item_id: str | None = None,
        phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS,
        field_name: str = "",
        relation_type: str = "",
    ):
        """Increment processed count for specific phase"""
        if phase == ProcessingPhase.MAIN_ITEMS:
            self.checkpoint.processed_items += 1
            self.checkpoint.processing_state.item_index += 1

        if item_id:
            self.checkpoint.last_processed_id = item_id
            self.checkpoint_manager.mark_item_processed(
                self.scraper_type,
                self.data_type,
                item_id,
                phase,
                field_name,
                relation_type,
            )

        self.checkpoint_manager.save_checkpoint(self.checkpoint)

    def increment_failed(
        self,
        item_id: str,
        error_message: str,
        error_details: str = "",
        phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS,
        field_name: str = "",
        relation_type: str = "",
    ):
        """Increment failed count and log error for specific phase"""
        self.checkpoint.failed_items += 1
        self.checkpoint_manager.log_error(
            self.scraper_type,
            self.data_type,
            item_id,
            error_message,
            error_details,
            phase,
            field_name,
            relation_type,
        )
        self.checkpoint_manager.save_checkpoint(self.checkpoint)

    def complete_processing(self):
        """Mark processing as completed"""
        self.checkpoint.status = CheckpointStatus.COMPLETED
        self.checkpoint.processing_state.phase = (
            ProcessingPhase.MAIN_ITEMS
        )  # Reset for next run
        self.checkpoint_manager.save_checkpoint(self.checkpoint)

        logger.info(f"Completed processing {self.scraper_type}:{self.data_type}")

    def fail_processing(self, error_message: str):
        """Mark processing as failed"""
        self.checkpoint.status = CheckpointStatus.FAILED
        self.checkpoint.processing_state.metadata["error"] = error_message
        self.checkpoint_manager.save_checkpoint(self.checkpoint)

        logger.error(
            f"Failed processing {self.scraper_type}:{self.data_type}: {error_message}"
        )

    def should_skip_item(
        self,
        item_id: str,
        phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS,
        field_name: str = "",
        relation_type: str = "",
    ) -> bool:
        """Check if an item should be skipped in a specific phase"""
        logger.debug(
            f"ProgressTracker.should_skip_item called: scraper_type={self.scraper_type}, data_type={self.data_type}, item_id={item_id}, phase={phase.value}"
        )

        result = self.checkpoint_manager.is_item_processed(
            self.scraper_type, self.data_type, item_id, phase, field_name, relation_type
        )

        logger.debug(f"ProgressTracker.should_skip_item result for {item_id}: {result}")
        return result

    def can_resume_from_phase(self, target_phase: ProcessingPhase) -> bool:
        """Check if we can resume from a specific processing phase"""
        current_phase = self.checkpoint.processing_state.phase
        # Define phase hierarchy - later phases can resume from earlier ones
        phase_order = [
            ProcessingPhase.MAIN_ITEMS,
            ProcessingPhase.NESTED_FIELDS,
            ProcessingPhase.RELATED_ENTITIES,
            ProcessingPhase.SECONDARY_RELATIONS,
        ]

        current_idx = phase_order.index(current_phase)
        target_idx = phase_order.index(target_phase)

        return target_idx >= current_idx

    @property
    def progress_percent(self) -> float:
        """Get progress percentage"""
        if self.checkpoint.total_items == 0:
            return 0.0
        return (self.checkpoint.processed_items / self.checkpoint.total_items) * 100

    @property
    def detailed_progress(self) -> dict[str, Any]:
        """Get detailed progress information including hierarchical state"""
        state = self.checkpoint.processing_state
        return {
            "overall_progress": self.progress_percent,
            "current_phase": state.phase.value,
            "item_progress": f"{state.item_index}/{state.item_total}",
            "field_progress": f"{state.field_index}/{state.field_total}",
            "relation_progress": f"{state.relation_index}/{state.relation_total}",
            "current_field": state.current_field_name,
            "current_relation": state.current_relation_type,
            "total_processed": self.checkpoint.processed_items,
            "total_failed": self.checkpoint.failed_items,
            "status": self.checkpoint.status.value,
        }

    # =============================================================================
    # CONVENIENCE METHODS FOR COMMON PROCESSING STAGES
    # =============================================================================

    @classmethod
    def create_for_stage(
        cls,
        checkpoint_manager: CheckpointManager,
        scraper_type: str,
        data_type: str,
        stage: ProcessingStage,
    ):
        """
        Create a progress tracker for a specific processing stage.

        This ensures consistent naming across different processing stages:
        - scraping: 'congressional', 'bills'
        - normalization: 'congressional', 'bills_normalizer'
        - cleaning: 'congressional', 'bills_cleaner'

        Args:
            checkpoint_manager: The checkpoint manager instance
            scraper_type: Type of scraper ('congressional', 'govinfo', etc.)
            data_type: Type of data ('bills', 'amendments', etc.)
            stage: Processing stage (SCRAPING, NORMALIZATION, CLEANING, etc.)

        Returns:
            HierarchicalProgressTracker configured for the stage
        """
        if stage == ProcessingStage.SCRAPING:
            tracker_data_type = data_type
        else:
            tracker_data_type = f"{data_type}_{stage.value}"

        return cls(checkpoint_manager, scraper_type, tracker_data_type)

    def get_items_needing_processing(
        self, available_items: list[str], rerun: bool = False
    ) -> list[str]:
        """
        Filter a list of available items to only those needing processing.

        Args:
            available_items: List of item IDs that are available for processing
            rerun: If True, return all items regardless of checkpoint status

        Returns:
            List of item IDs that need processing
        """
        if rerun:
            return available_items

        items_to_process = []
        for item_id in available_items:
            if not self.should_skip_item(item_id, ProcessingPhase.MAIN_ITEMS):
                items_to_process.append(item_id)

        return items_to_process

    def get_processing_summary(self) -> dict[str, Any]:
        """
        Get a summary of processing status including counts and percentages.

        Returns:
            Dictionary with processing summary information
        """
        total = self.checkpoint.total_items
        processed = self.checkpoint.processed_items
        failed = self.checkpoint.failed_items
        remaining = max(0, total - processed - failed)

        return {
            "data_type": self.data_type,
            "scraper_type": self.scraper_type,
            "status": self.checkpoint.status.value,
            "total_items": total,
            "processed_items": processed,
            "failed_items": failed,
            "remaining_items": remaining,
            "progress_percent": self.progress_percent,
            "last_processed_id": self.checkpoint.last_processed_id,
            "last_processed_date": self.checkpoint.last_processed_date,
            "created_at": self.checkpoint.created_at,
            "updated_at": self.checkpoint.updated_at,
        }

    def reset_if_failed(self) -> bool:
        """
        Reset the checkpoint if it's in a failed state.

        Returns:
            True if checkpoint was reset, False if no reset was needed
        """
        if self.checkpoint.status == CheckpointStatus.FAILED:
            self.checkpoint_manager.reset_checkpoint(self.scraper_type, self.data_type)
            # Reload the checkpoint after reset
            self.checkpoint = self.checkpoint_manager.load_checkpoint(
                self.scraper_type, self.data_type
            )
            if not self.checkpoint:
                self.checkpoint = CheckpointData(
                    scraper_type=self.scraper_type,
                    data_type=self.data_type,
                    status=CheckpointStatus.PENDING,
                    processing_state=ProcessingState(),
                )
                self.checkpoint_manager.save_checkpoint(self.checkpoint)

            logger.info(
                f"Reset failed checkpoint for {self.scraper_type}:{self.data_type}"
            )
            return True
        return False

    def mark_item_completed(
        self,
        item_id: str,
        phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS,
        field_name: str = "",
        relation_type: str = "",
    ) -> None:
        """
        Mark an item as completed (legacy method for backward compatibility).

        This method provides the same functionality as increment_processed
        but with a different name for backward compatibility.
        """
        self.increment_processed(item_id, phase, field_name, relation_type)

    async def is_item_completed(
        self,
        item_id: str,
        phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS,
        field_name: str = "",
        relation_type: str = "",
    ) -> bool:
        """
        Check if an item is completed (legacy method for backward compatibility).

        This method provides the same functionality as should_skip_item
        but with a different name and async signature for backward compatibility.
        """
        return self.should_skip_item(item_id, phase, field_name, relation_type)


# Legacy ProgressTracker for backward compatibility
class ProgressTracker(HierarchicalProgressTracker):
    """Legacy progress tracker - provides backward compatibility"""


if __name__ == "__main__":
    # Example usage demonstrating hierarchical tracking
    logging.basicConfig(level=logging.INFO)

    # Create checkpoint manager
    manager = CheckpointManager()

    # Create hierarchical progress tracker
    tracker = HierarchicalProgressTracker(manager, "congressional", "bills")

    # Simulate hierarchical processing
    tracker.start_processing(total_items=100)

    # Processing main items
    tracker.set_processing_phase(ProcessingPhase.MAIN_ITEMS, item_total=100)

    for i in range(5):
        item_id = f"bill_{i}"
        if not tracker.should_skip_item(item_id, ProcessingPhase.MAIN_ITEMS):
            # Process nested fields
            tracker.set_processing_phase(
                ProcessingPhase.NESTED_FIELDS,
                item_index=i,
                item_total=100,
                field_index=0,
                field_total=3,
            )

            for field_idx, field_name in enumerate(
                ["sponsors", "actions", "committees"]
            ):
                tracker.set_processing_phase(
                    ProcessingPhase.NESTED_FIELDS,
                    item_index=i,
                    item_total=100,
                    field_index=field_idx,
                    field_total=3,
                    current_field=field_name,
                )
                # Simulate nested field processing
                logger.info(f"Processing {field_name} for {item_id}")

            # Process related entities
            tracker.set_processing_phase(
                ProcessingPhase.RELATED_ENTITIES,
                item_index=i,
                item_total=100,
                relation_index=0,
                relation_total=2,
            )

            for rel_idx, rel_type in enumerate(["amendments", "votes"]):
                tracker.set_processing_phase(
                    ProcessingPhase.RELATED_ENTITIES,
                    item_index=i,
                    item_total=100,
                    relation_index=rel_idx,
                    relation_total=2,
                    current_relation=rel_type,
                )
                # Simulate related entity processing
                logger.info(f"Processing {rel_type} for {item_id}")

            # Mark main item as processed
            tracker.increment_processed(item_id, ProcessingPhase.MAIN_ITEMS)
            logger.info(f"Completed {item_id}: {tracker.detailed_progress}")

    tracker.complete_processing()

    # Show summary
    summary = manager.get_progress_summary("congressional")
    logger.info(f"Summary: {summary}")
