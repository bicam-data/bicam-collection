"""PostgreSQL-backed CheckpointManager.

This module provides a drop-in replacement for the SQLite-based
`CheckpointManager` found in `libs.checkpoint`.  The public API is
kept identical (synchronous methods) so higher-level code can switch
between the two back-ends purely through configuration.

Only the most frequently used methods are fully implemented.  The
remaining ones raise `NotImplementedError` for now – they can be
completed incrementally.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import psycopg2.extras as _pg
from psycopg2.pool import SimpleConnectionPool

from .checkpoint import (
    CheckpointData,
    CheckpointStatus,
    ProcessingPhase,
    ProcessingState,
)

logger = logging.getLogger(__name__)


class PostgresCheckpointManager:  # noqa: D101 – external API mirrors sqlite version
    def __init__(
        self,
        *,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        schema: str = "bicam_checkpoints",
        minconn: int = 1,
        maxconn: int = 10,
    ) -> None:
        self.schema = schema
        self._pool = SimpleConnectionPool(
            minconn,
            maxconn,
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            cursor_factory=_pg.DictCursor,
        )
        logger.info(
            "PostgresCheckpointManager initialised (schema=%s, pool=%s→%s)",
            schema,
            minconn,
            maxconn,
        )
        self._init_db()

    # ------------------------------------------------------------------
    #  Internal helpers
    # ------------------------------------------------------------------
    def _get_conn(self):
        return self._pool.getconn()

    def _put_conn(self, conn):
        self._pool.putconn(conn)

    def _execute(
        self, sql: str, params: Sequence[Any] | None = None, fetch: bool = False
    ):
        params = params or ()
        conn = self._get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch:
                    return cur.fetchall()
                return None
        finally:
            self._put_conn(conn)

    # ------------------------------------------------------------------
    def _init_db(self) -> None:
        ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema};
        CREATE TABLE IF NOT EXISTS {self.schema}.checkpoints (
            id SERIAL PRIMARY KEY,
            scraper_type TEXT NOT NULL,
            data_type TEXT NOT NULL,
            last_processed_id TEXT,
            last_processed_date TEXT,
            total_items INTEGER DEFAULT 0,
            processed_items INTEGER DEFAULT 0,
            failed_items INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            processing_state JSONB DEFAULT '{{}}',
            api_params JSONB DEFAULT '{{}}',
            last_exported_item JSONB DEFAULT '{{}}',
            batch_info JSONB DEFAULT '{{}}',
            metadata JSONB DEFAULT '{{}}',
            UNIQUE(scraper_type, data_type)
        );

        CREATE TABLE IF NOT EXISTS {self.schema}.processed_items (
            id SERIAL PRIMARY KEY,
            checkpoint_id INTEGER REFERENCES {self.schema}.checkpoints(id) ON DELETE CASCADE,
            item_id TEXT NOT NULL,
            processing_phase TEXT NOT NULL,
            field_name TEXT,
            relation_type TEXT,
            processed_at TIMESTAMPTZ NOT NULL,
            UNIQUE(checkpoint_id, item_id, processing_phase, field_name, relation_type)
        );

        CREATE TABLE IF NOT EXISTS {self.schema}.checkpoint_errors (
            id SERIAL PRIMARY KEY,
            checkpoint_id INTEGER REFERENCES {self.schema}.checkpoints(id) ON DELETE CASCADE,
            item_id TEXT,
            error_message TEXT,
            error_details TEXT,
            processing_phase TEXT,
            field_name TEXT,
            relation_type TEXT,
            created_at TIMESTAMPTZ NOT NULL
        );
        """
        self._execute(ddl)

    # ------------------------------------------------------------------
    #  Public API (subset)
    # ------------------------------------------------------------------
    def save_checkpoint(self, cp: CheckpointData) -> int:
        cp.updated_at = datetime.now(UTC).isoformat()
        if not cp.created_at:
            cp.created_at = cp.updated_at

        row = self._execute(
            f"""
            INSERT INTO {self.schema}.checkpoints
            (scraper_type, data_type, last_processed_id, last_processed_date,
                total_items, processed_items, failed_items, status,
                created_at, updated_at, processing_state, api_params,
                last_exported_item, batch_info, metadata)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb)
            ON CONFLICT (scraper_type,data_type)
            DO UPDATE SET
              last_processed_id   = EXCLUDED.last_processed_id,
              last_processed_date = EXCLUDED.last_processed_date,
              total_items         = EXCLUDED.total_items,
              processed_items     = EXCLUDED.processed_items,
              failed_items        = EXCLUDED.failed_items,
              status              = EXCLUDED.status,
              updated_at          = EXCLUDED.updated_at,
              processing_state    = EXCLUDED.processing_state,
              api_params          = EXCLUDED.api_params,
              last_exported_item  = EXCLUDED.last_exported_item,
              batch_info          = EXCLUDED.batch_info,
              metadata            = EXCLUDED.metadata
            RETURNING id;
            """,
            (
                cp.scraper_type,
                cp.data_type,
                cp.last_processed_id,
                cp.last_processed_date,
                cp.total_items,
                cp.processed_items,
                cp.failed_items,
                cp.status.value,
                cp.created_at,
                cp.updated_at,
                json.dumps(cp.processing_state.__dict__, default=str),
                json.dumps(cp.api_params or {}),
                json.dumps(cp.last_exported_item or {}),
                json.dumps(cp.batch_info or {}),
                json.dumps(cp.metadata or {}),
            ),
            fetch=True,
        )
        return row[0]["id"]

    # ------------------------------------------------------
    def load_checkpoint(
        self, scraper_type: str, data_type: str
    ) -> CheckpointData | None:
        rows = self._execute(
            f"SELECT * FROM {self.schema}.checkpoints WHERE scraper_type=%s AND data_type=%s",
            (scraper_type, data_type),
            fetch=True,
        )
        if not rows:
            return None
        return self._row_to_cp(rows[0])

    # ------------------------------------------------------
    def _get_checkpoint_id(self, scraper: str, data_type: str) -> int | None:
        rows = self._execute(
            f"SELECT id FROM {self.schema}.checkpoints WHERE scraper_type=%s AND data_type=%s",
            (scraper, data_type),
            fetch=True,
        )
        return rows[0]["id"] if rows else None

    # ------------------------------------------------------
    def mark_item_processed(
        self,
        scraper_type: str,
        data_type: str,
        item_id: str,
        phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS,
        field_name: str = "",
        relation_type: str = "",
    ) -> None:
        cid = self._get_checkpoint_id(scraper_type, data_type)
        if not cid:
            return
        self._execute(
            f"""
            INSERT INTO {self.schema}.processed_items
            (checkpoint_id,item_id,processing_phase,field_name,relation_type,processed_at)
            VALUES (%s,%s,%s,%s,%s, now())
            ON CONFLICT DO NOTHING;
            """,
            (cid, item_id, phase.value, field_name, relation_type),
        )

    # ------------------------------------------------------
    def is_item_processed(
        self,
        scraper_type: str,
        data_type: str,
        item_id: str,
        phase: ProcessingPhase = ProcessingPhase.MAIN_ITEMS,
        field_name: str = "",
        relation_type: str = "",
    ) -> bool:
        cid = self._get_checkpoint_id(scraper_type, data_type)
        if not cid:
            return False
        rows = self._execute(
            f"""
            SELECT 1 FROM {self.schema}.processed_items
            WHERE checkpoint_id=%s AND item_id=%s AND processing_phase=%s
                AND field_name=%s AND relation_type=%s
            LIMIT 1;
            """,
            (cid, item_id, phase.value, field_name, relation_type),
            fetch=True,
        )
        return bool(rows)

    # ------------------------------------------------------
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
        cid = self._get_checkpoint_id(scraper_type, data_type)
        if not cid:
            return
        self._execute(
            f"""
            INSERT INTO {self.schema}.checkpoint_errors
            (checkpoint_id,item_id,error_message,error_details,processing_phase,
                field_name,relation_type,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s, now());
            """,
            (
                cid,
                item_id,
                error_message,
                error_details,
                phase.value,
                field_name,
                relation_type,
            ),
        )

    # ------------------------------------------------------
    #  Helper
    # ------------------------------------------------------
    def _row_to_cp(self, r) -> CheckpointData:  # noqa: D401 – util
        ps = r["processing_state"] or {}
        if isinstance(ps, str):
            ps = json.loads(ps)

        checkpoint_data = CheckpointData(
            scraper_type=r["scraper_type"],
            data_type=r["data_type"],
            last_processed_id=r["last_processed_id"],
            last_processed_date=r["last_processed_date"],
            total_items=r["total_items"],
            processed_items=r["processed_items"],
            failed_items=r["failed_items"],
            status=CheckpointStatus(r["status"]),
            created_at=r["created_at"].isoformat(),
            updated_at=r["updated_at"].isoformat(),
            processing_state=ProcessingState(
                phase=ProcessingPhase(
                    ps.get("phase", ProcessingPhase.MAIN_ITEMS.value)
                ),
                item_index=ps.get("item_index", 0),
                item_total=ps.get("item_total", 0),
                field_index=ps.get("field_index", 0),
                field_total=ps.get("field_total", 0),
                relation_index=ps.get("relation_index", 0),
                relation_total=ps.get("relation_total", 0),
                secondary_index=ps.get("secondary_index", 0),
                secondary_total=ps.get("secondary_total", 0),
                current_field_name=ps.get("current_field_name", ""),
                current_relation_type=ps.get("current_relation_type", ""),
                metadata=ps.get("metadata", {}),
            ),
            api_params=r["api_params"],
            last_exported_item=r["last_exported_item"],
            batch_info=r["batch_info"],
            metadata=r["metadata"],
        )

        # Add the database ID as a dynamic attribute for PostgreSQL compatibility
        # This allows the commands.py to access checkpoint.id
        checkpoint_data.id = r["id"]

        return checkpoint_data

    def reset_checkpoint(self, scraper_type: str, data_type: str) -> None:  # noqa: D401
        cid = self._get_checkpoint_id(scraper_type, data_type)
        if not cid:
            return
        # Fresh processing_state json
        fresh_state = json.dumps(
            {
                "phase": ProcessingPhase.MAIN_ITEMS.value,
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

        self._execute(
            f"""
            DELETE FROM {self.schema}.processed_items WHERE checkpoint_id=%s;
            DELETE FROM {self.schema}.checkpoint_errors WHERE checkpoint_id=%s;
            UPDATE {self.schema}.checkpoints
                SET last_processed_id = NULL,
                    last_processed_date = NULL,
                    processed_items = 0,
                    failed_items = 0,
                    status = 'pending',
                    processing_state = %s::jsonb,
                    api_params = '{{}}',
                    last_exported_item = '{{}}',
                    batch_info = '{{}}',
                    updated_at = now()
                WHERE id = %s;
            """,
            (cid, cid, fresh_state, cid),
        )

    def delete_checkpoint(self, scraper_type: str, data_type: str) -> None:  # noqa: D401
        cid = self._get_checkpoint_id(scraper_type, data_type)
        if not cid:
            return
        self._execute(
            f"""
            DELETE FROM {self.schema}.processed_items WHERE checkpoint_id=%s;
            DELETE FROM {self.schema}.checkpoint_errors WHERE checkpoint_id=%s;
            DELETE FROM {self.schema}.checkpoints WHERE id=%s;
            """,
            (cid, cid, cid),
        )

    # ------------------------------------------------------
    # Missing methods needed by the command interface
    # ------------------------------------------------------

    def get_checkpoints_for_data_type(
        self, data_type: str, stage_name: str | None = None
    ) -> list[CheckpointData]:
        """Get checkpoints for a specific data type, optionally filtered by stage."""
        if stage_name:
            # Based on the HierarchicalProgressTracker.create_for_stage method:
            # - scraping: data_type = "bills"
            # - normalization: data_type = "bills_normalization"
            # - cleaning: data_type = "bills_cleaning"
            # - analysis: data_type = "bills_analysis"

            if stage_name == "scraping":
                # For scraping stage, use exact data_type match
                target_data_type = data_type
            else:
                # For other stages, use pattern: "{data_type}_{stage_name}"
                target_data_type = f"{data_type}_{stage_name}"

            rows = self._execute(
                f"""
                SELECT * FROM {self.schema}.checkpoints
                WHERE data_type = %s
                ORDER BY created_at DESC
                """,
                (target_data_type,),
                fetch=True,
            )
        else:
            # No stage filter, get all checkpoints related to this data type
            # This includes the base data type and all stage variations
            rows = self._execute(
                f"""
                SELECT * FROM {self.schema}.checkpoints 
                WHERE data_type = %s 
                   OR data_type LIKE %s
                ORDER BY created_at DESC
                """,
                (data_type, f"{data_type}_%"),
                fetch=True,
            )

        return [self._row_to_cp(row) for row in rows] if rows else []

    def clear_checkpoint(self, checkpoint_id: int) -> None:
        """Clear/delete a specific checkpoint by ID."""
        self._execute(
            f"""
            DELETE FROM {self.schema}.processed_items WHERE checkpoint_id = %s;
            DELETE FROM {self.schema}.checkpoint_errors WHERE checkpoint_id = %s;
            DELETE FROM {self.schema}.checkpoints WHERE id = %s;
            """,
            (checkpoint_id, checkpoint_id, checkpoint_id),
        )

    def list_checkpoints(self, scraper_type: str | None = None) -> list[CheckpointData]:
        """List all checkpoints, optionally filtered by scraper_type."""
        if scraper_type:
            rows = self._execute(
                f"""
                SELECT * FROM {self.schema}.checkpoints
                WHERE scraper_type = %s
                ORDER BY created_at DESC
                """,
                (scraper_type,),
                fetch=True,
            )
        else:
            rows = self._execute(
                f"""
                SELECT * FROM {self.schema}.checkpoints
                ORDER BY created_at DESC
                """,
                fetch=True,
            )

        return [self._row_to_cp(row) for row in rows] if rows else []
