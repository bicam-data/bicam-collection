from collections.abc import Mapping

import polars as pl

# ---------------------------------------------------------------------------
# Identification
# ---------------------------------------------------------------------------

# Key name as it appears in raw JSON and after renaming
RAW_ID_FIELD = "billId"
ID_ALIAS = "bill_id"

# ---------------------------------------------------------------------------
# Column mappings
# ---------------------------------------------------------------------------

FIELD_MAP: Mapping[str, str] = {
    RAW_ID_FIELD: ID_ALIAS,
    "billNumber": "bill_number",
    "billType": "bill_type",
    "congress": "congress",
    "title": "title",
    "originChamber": "origin_chamber",
    "introducedDate": "introduced_at",
    "isLaw": "is_law",
    "actions.count": "actions_count",
    "actions.url": "actions_url",
    "amendments.count": "amendments_count",
    "amendments.url": "amendments_url",
    "committees.count": "committees_count",
    "committees.url": "committees_url",
    "cosponsors.count": "cosponsors_count",
    "cosponsors.url": "cosponsors_url",
    "relatedBills.count": "bill_relations_count",
    "relatedBills.url": "bill_relations_url",
    "subjects.count": "subjects_count",
    "subjects.url": "subjects_url",
    "summaries.count": "summaries_count",
    "summaries.url": "summaries_url",
    "titles.count": "titles_count",
    "titles.url": "titles_url",
    "textVersions.count": "texts_count",
    "textVersions.url": "texts_url",
    "notes.text": "notes_text",
}

# Primary/unique columns for database writes
KEY_COLUMNS: list[str] = [ID_ALIAS]


# ---------------------------------------------------------------------------
# Custom Polars transforms
# ---------------------------------------------------------------------------

def custom_transforms(df: pl.LazyFrame) -> pl.LazyFrame:
    """Apply additional Polars expressions specific to bills."""
    return (
        df
        .with_columns([
            pl.col("origin_chamber").str.to_lowercase(),
            pl.col("congress").cast(pl.Int64, strict=False),
        ])
    )

# ---------------------------------------------------------------------------
# Child endpoints (require extra API calls)
# ---------------------------------------------------------------------------

CHILD_ENDPOINTS: dict[str, dict] = {
    "actions": {
        "url_field": "actions_url",
        "id_field": "actionId",  # key in child JSON
        "raw_schema": "bicam_raw_congressional",
    },
    "amendments": {
        "url_field": "amendments_url",
        "id_field": "amendmentId",
        "raw_schema": "bicam_raw_congressional",
    },
    "committees": {
        "url_field": "committees_url",
        "id_field": "committeeId",
        "raw_schema": "bicam_raw_congressional",
    },
    "cosponsors": {
        "url_field": "cosponsors_url",
        "id_field": "bioguideId",
        "raw_schema": "bicam_raw_congressional",
    },
    "billrelations": {
        "url_field": "bill_relations_url",
        "id_field": "billId",
        "raw_schema": "bicam_raw_congressional",
    },
    "subjects": {
        "url_field": "subjects_url",
        "id_field": "subjectId",
        "raw_schema": "bicam_raw_congressional",
    },
    "summaries": {
        "url_field": "summaries_url",
        "id_field": "summaryId",
        "raw_schema": "bicam_raw_congressional",
    },
    "titles": {
        "url_field": "titles_url",
        "id_field": "titleId",
        "raw_schema": "bicam_raw_congressional",
    },
    "texts": {
        "url_field": "texts_url",
        "id_field": "textVersionId",
        "raw_schema": "bicam_raw_congressional",
    },
}

# ---------------------------------------------------------------------------
# Nested arrays within parent payload
# ---------------------------------------------------------------------------

NESTED_ARRAYS: dict[str, dict] = {
    "cbo_cost_estimates": {
        "json_path": "$.cboCostEstimates[*]",
        "field_map": {
            "id": "cbo_id",
            "pubDate": "pub_date",
            "title": "title",
            "url": "url",
        },
        "key_columns": [ID_ALIAS, "cbo_id"],
    },
    "laws": {
        "json_path": "$.laws[*]",
        "field_map": {
            "lawNumber": "law_number",
            "lawType": "law_type",
            "citation": "citation",
        },
        "key_columns": [ID_ALIAS, "citation"],
    },
}
