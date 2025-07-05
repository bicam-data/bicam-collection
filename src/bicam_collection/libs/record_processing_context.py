from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


@dataclass
class RecordProcessingContext:
    """Encapsulates runtime information required while normalising records.

    The class is intentionally **agnostic** of any particular database or
    pipeline implementation – it merely keeps bookkeeping information and
    provides a few convenience helpers, so it can be re-used by other
    normaliser implementations as well.
    """

    record_type: str  # "main", "related", "nested"
    data_type_name: str
    main_table_name: str
    main_id_field: str

    # Applicable only for *related* / *nested* records
    table_suffix: str | None = None
    parent_id: str | None = None  # Keep for backward compatibility
    id_field: str = "parent_id"  # Configurable field name for parent reference

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    @staticmethod
    def generate_content_hash(flat_item: dict[str, Any], base_record_id: str) -> str:
        """Return a short, deterministic content hash for *flat_item*.

        The hash is used as a surrogate/stable identifier when the source data
        does not provide one.  By incorporating *base_record_id* we get better
        cardinality and avoid cross-item collisions.
        """
        content_items: list[str] = [f"parent:{base_record_id}"]
        for key in sorted(flat_item):
            value = flat_item[key]
            if value is not None:
                content_items.append(f"{key}:{value}")
        content_string = "|".join(content_items)
        return hashlib.sha256(content_string.encode()).hexdigest()[:16]

    # ------------------------------------------------------------------
    # Relationship / naming helpers
    # ------------------------------------------------------------------
    def add_relationships(self, flat_item: dict[str, Any], base_record_id: str) -> None:
        """Mutate *flat_item* in-place to carry the correct foreign keys."""
        if self.record_type != "main":
            flat_item[self.main_id_field] = base_record_id

    def get_extraction_params(self) -> dict[str, Any]:
        """Return the keyword parameters expected by ``_extract_lists``."""
        if self.record_type == "main":
            return {"is_nested": False}
        return {
            "is_nested": True,
            "parent_table": self.table_suffix,
            "root_id": self.parent_id,
        }

    def get_table_name(self) -> str:
        """Return the final database table name that *flat_item* belongs to."""
        if self.record_type == "main":
            return self.main_table_name
        return f"{self.data_type_name}_{self.table_suffix}"
