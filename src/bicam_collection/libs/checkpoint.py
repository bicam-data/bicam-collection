"""Compatibility shim providing old bicam_collection.libs.checkpoint public API.

This wrapper maps the legacy *CheckpointManager* / *HierarchicalProgressTracker*
interface onto the new :pymod:`bicam_collection.libs.hierarchical_checkpoint_system`
implementation so that existing import paths keep working while the code-base is
migrated.

NOTE: Only the subset of the API still used by the actively maintained modules
is implemented.  If you hit a *NotImplementedError* simply extend the shim.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from bicam_collection.libs.hierarchical_checkpoint_system import (
    CleaningPhase,
    FetchingPhase,
    HierarchicalCheckpointManager,
    ProcessingStage,
    StagingPhase,
)

# ---------------------------------------------------------------------------
# Re-export new manager under the old name
# ---------------------------------------------------------------------------

CheckpointManager = HierarchicalCheckpointManager  # type: ignore

# ---------------------------------------------------------------------------
# Legacy status / phase enums (very lightweight)
# ---------------------------------------------------------------------------


class CheckpointStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ProcessingPhase(str, Enum):
    """Generic phase enumeration retained for backward-compatibility."""

    PHASE_1 = "phase_1"
    PHASE_2 = "phase_2"
    PHASE_3 = "phase_3"
    MAIN_ITEMS = "main_items"
    RELATED_ENTITIES = "related_entities"


# ---------------------------------------------------------------------------
# Thin progress-tracker built on top of the new *HierarchicalCheckpointManager*
# ---------------------------------------------------------------------------


class HierarchicalProgressTracker:
    """Minimal wrapper used by legacy fetcher / cleaner / normalizer classes."""

    def __init__(
        self,
        checkpoint_manager: HierarchicalCheckpointManager,
        system_name: str,
        data_type: str,
        stage: ProcessingStage,
    ) -> None:
        self._cm = checkpoint_manager
        self.system_name = system_name
        self.data_type = data_type
        self.stage = stage
        self.current_phase: str | None = None  # textual representation

    # ------------------------------------------------------------------
    # Class helpers
    # ------------------------------------------------------------------
    @classmethod
    def create_for_stage(
        cls,
        checkpoint_manager: HierarchicalCheckpointManager,
        system_name: str,
        data_type: str,
        stage: ProcessingStage,
    ) -> "HierarchicalProgressTracker":
        return cls(checkpoint_manager, system_name, data_type, stage)

    # ------------------------------------------------------------------
    # Phase helpers
    # ------------------------------------------------------------------
    def set_processing_phase(self, phase: ProcessingPhase | str) -> None:
        self.current_phase = phase.value if isinstance(phase, Enum) else str(phase)

    async def start_phase(self, phase: ProcessingPhase | str):  # noqa: D401
        self.set_processing_phase(phase)

    async def complete_phase(self, phase: ProcessingPhase | str):  # noqa: D401
        # For now we only persist that the *phase* completed – more fine-grained
        # metrics can be added when necessary.
        phase_name = phase.value if isinstance(phase, Enum) else str(phase)
        cp = self._cm.get_or_create_checkpoint(self.stage, phase_name, self.data_type)
        cp.processed_items = cp.total_items  # naive but fine for shim
        self._cm.save_checkpoint(cp)

    # ------------------------------------------------------------------
    # Item-level helpers – we only implement what the callers use
    # ------------------------------------------------------------------
    def should_skip_item(
        self,
        self_,
        item_id: str,
        phase: ProcessingPhase | str | None = None,
        *,
        field_name: str = "",
    ) -> bool:
        phase_name = (
            (phase.value if isinstance(phase, Enum) else str(phase))
            if phase is not None
            else (self_.current_phase or "")
        )
        return self_._cm.is_item_processed(
            self_.stage, phase_name, self_.data_type, item_id, sub_item=field_name
        )

    def mark_item_processed(
        self,
        item_id: str,
        phase: ProcessingPhase | str | None = None,
        *,
        field_name: str = "",
    ) -> None:
        phase_name = (
            (phase.value if isinstance(phase, Enum) else str(phase))
            if phase is not None
            else (self.current_phase or "")
        )
        self._cm.mark_item_processed(
            self.stage, phase_name, self.data_type, item_id, sub_item=field_name
        )

    # Aliases used by some modules ------------------------------------------------
    should_skip_item = should_skip_item  # type: ignore  # keep linters happy

    def increment_processed(self, count: int = 1):
        # Legacy API accepted *count* but the information is no longer stored
        # at this granularity.
        pass

    def update_progress(self, processed: int, total: int | None = None):
        # No-op shim – extend if fine-grained metrics are required.
        pass

    # ------------------------------------------------------------------
    # Compatibility attribute(s)
    # ------------------------------------------------------------------
    @property
    def checkpoint(self):  # noqa: D401 – compatibility property
        if self.current_phase is None:
            return None
        return self._cm.get_or_create_checkpoint(
            self.stage, self.current_phase, self.data_type
        )


# ---------------------------------------------------------------------------
# Backwards-compat convenience re-exports (phases from new module) -----------
# ---------------------------------------------------------------------------

FetchingPhase = FetchingPhase  # type: ignore
StagingPhase = StagingPhase  # type: ignore
CleaningPhase = CleaningPhase  # type: ignore
