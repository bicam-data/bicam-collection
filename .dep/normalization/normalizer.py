"""Generic normaliser that moves data from *raw* JSONB tables to
*intermediate* relational tables using declarative Polars specs.

It also:
• fetches child-endpoint payloads defined in the spec (CHILD_ENDPOINTS)
  and stores them in their own raw tables.
• explodes nested arrays (NESTED_ARRAYS) from the parent JSON into
  dedicated intermediate tables.

Only the `FIELD_MAP`, `KEY_COLUMNS` and optional CHILD/NESTED configs need
setting per-data-type.  See `README_NORMALIZATION.md` for details.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import aiohttp
import polars as pl
import psycopg2
from psycopg2 import sql as _psql
from psycopg2.extras import RealDictCursor

from bicam_collection.cleaning.cleaning_utils.insert_data import write_data

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema maps – extend when you add new data types / sources
# ---------------------------------------------------------------------------
RAW_SCHEMA_MAP: dict[str, str] = {
    "bills": "bicam_raw_congressional",
    "bills_actions": "bicam_raw_congressional",
}

INTERMEDIATE_SCHEMA_MAP: dict[str, str] = {
    "bills": "bicam_intermediate_congressional",
    "bills_actions": "bicam_intermediate_congressional",
}


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def load_spec(data_type: str):
    """Import `<package>.normalization.specs.<data_type>`"""
    return importlib.import_module(f"bicam_collection.normalization.specs.{data_type}")


def fetch_raw_payloads(conn, schema: str, table: str):
    """Yield dicts: each top-level key from JSON becomes a column."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            _psql.SQL("""SELECT id, payload FROM {}.{}""").format(
                _psql.Identifier(schema), _psql.Identifier(table)
            )
        )
        for row in cur:
            payload = row["payload"] or {}
            payload["id"] = row["id"]  # keep primary id around
            yield payload


async def _fetch_one(
    session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore
) -> dict[str, Any] | None:
    async with sem:
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=60)
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                data["endpoint"] = url
                return data
        except Exception as exc:
            logger.warning(f"⚠️  fetch failed for {url}: {exc}")
            return None


async def fetch_urls_in_bulk(
    urls: list[str], max_conn: int = 32
) -> list[dict[str, Any]]:
    """Fetch many URLs concurrently → list of JSON dicts (skips failures)."""
    sem = asyncio.Semaphore(max_conn)
    async with aiohttp.ClientSession() as sess:
        tasks = [_fetch_one(sess, u, sem) for u in urls]
        results = await asyncio.gather(*tasks, return_exceptions=False)
    return [r for r in results if r]


def insert_raw_jsonb(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    items: list[dict[str, Any]],
    id_field: str,
):
    if not items:
        return

    with conn.cursor() as cur:
        cur.execute(
            _psql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(
                _psql.Identifier(schema)
            )
        )
        cur.execute(
            _psql.SQL(
                """CREATE TABLE IF NOT EXISTS {}.{} (
                        id TEXT PRIMARY KEY,
                        fetched_at TIMESTAMPTZ NOT NULL,
                        payload JSONB NOT NULL,
                        endpoint TEXT)
                """
            ).format(_psql.Identifier(schema), _psql.Identifier(table))
        )

        now = datetime.now(UTC)
        records = []
        for itm in items:
            pk_val = itm.get(id_field)
            if pk_val is None:
                continue
            records.append(
                (
                    str(pk_val),
                    now,
                    json.dumps(itm),
                    itm.get("endpoint"),
                )
            )

        if not records:
            return
        insert_q = _psql.SQL(
            """INSERT INTO {}.{} (id, fetched_at, payload, endpoint)
                VALUES (%s,%s,%s::jsonb,%s)
                ON CONFLICT (id) DO NOTHING"""
        ).format(_psql.Identifier(schema), _psql.Identifier(table))
        psycopg2.extras.execute_batch(cur, insert_q, records, page_size=500)
        conn.commit()


# ---------------------------------------------------------------------------
# Core routine
# ---------------------------------------------------------------------------


def normalize_data_type(
    data_type: str, source: str = "congressional", table_type: str = "all"
):
    """Enhanced normalization supporting 4-phase approach.

    Args:
        data_type: The data type to normalize (e.g., 'bills', 'amendments')
        source: The data source ('congressional' or 'govinfo')
        table_type: Type of normalization ('main', 'nested', or 'all')
    """

    # Enhanced schema mapping for multiple sources
    raw_schema = f"bicam_raw_{source}"
    intermediate_schema = f"bicam_intermediate_{source}"

    try:
        spec = load_spec(data_type)
    except ImportError:
        logger.warning(
            f"No normalization spec found for {data_type}, using default processing"
        )
        return {
            "data_type": data_type,
            "source": source,
            "normalized_rows": 0,
            "nested_tables": 0,
            "total_nested_rows": 0,
            "success": False,
            "error": f"No spec for {data_type}",
        }

    raw_table = f"{data_type}_raw"

    conn = psycopg2.connect(
        host=os.getenv("POSTGRESQL_HOST"),
        port=os.getenv("POSTGRESQL_PORT"),
        database=os.getenv("POSTGRESQL_DATABASE"),
        user=os.getenv("POSTGRESQL_USERNAME"),
        password=os.getenv("POSTGRESQL_PASSWORD"),
    )

    result = {
        "data_type": data_type,
        "source": source,
        "table_type": table_type,
        "normalized_rows": 0,
        "nested_tables": 0,
        "total_nested_rows": 0,
        "success": True,
    }

    try:
        rows = list(fetch_raw_payloads(conn, raw_schema, raw_table))
        if not rows:
            logger.warning(f"No rows in {raw_schema}.{raw_table}")
            return result

        # Process main table normalization
        if table_type in ["main", "all"]:
            result.update(
                normalize_main_table(rows, spec, data_type, intermediate_schema)
            )

        # Process nested table normalization
        if table_type in ["nested", "all"]:
            nested_result = normalize_nested_tables(
                rows, spec, data_type, intermediate_schema, conn
            )
            result["nested_tables"] = nested_result["nested_tables"]
            result["total_nested_rows"] = nested_result["total_nested_rows"]

    except Exception as e:
        logger.error(f"Error normalizing {data_type} from {source}: {e}")
        result["success"] = False
        result["error"] = str(e)
    finally:
        conn.close()

    return result


def normalize_main_table(rows, spec, data_type: str, intermediate_schema: str):
    """Normalize the main parent table."""

    df = pl.from_dicts(rows)
    lazy = df.lazy().select(
        [
            pl.col(src).alias(dst)
            for src, dst in spec.FIELD_MAP.items()
            if src in df.columns
        ]
    )

    if hasattr(spec, "custom_transforms"):
        lazy = spec.custom_transforms(lazy)

    parent_df = lazy.collect().to_pandas()

    write_data(
        parent_df,
        f"{intermediate_schema}.{data_type}",
        key_columns=getattr(spec, "KEY_COLUMNS", []),
        if_exists="truncate",
    )

    logger.info(
        f"✅ Main table {intermediate_schema}.{data_type} ← {len(parent_df)} rows"
    )

    return {
        "normalized_rows": len(parent_df),
    }


def normalize_nested_tables(rows, spec, data_type: str, intermediate_schema: str, conn):
    """Normalize nested tables from JSON arrays."""

    nested_tables = 0
    total_nested_rows = 0

    # Process child endpoints
    child_cfg = getattr(spec, "CHILD_ENDPOINTS", {})
    if child_cfg:
        for name, cfg in child_cfg.items():
            url_col = cfg["url_field"]
            id_field = cfg["id_field"]
            child_schema = cfg["raw_schema"]

            df = pl.from_dicts(rows)
            if url_col not in df.columns:
                continue

            url_list = (
                df.select(pl.col(url_col)).drop_nulls().unique().to_series().to_list()
            )

            if not url_list:
                continue

            payloads = asyncio.run(fetch_urls_in_bulk(url_list))
            insert_raw_jsonb(
                conn, child_schema, f"{data_type}_{name}_raw", payloads, id_field
            )
            logger.info(
                f"   ↳ stored {len(payloads)} child payloads in {child_schema}.{data_type}_{name}_raw"
            )
            nested_tables += 1

    # Process nested arrays
    nested_cfg = getattr(spec, "NESTED_ARRAYS", {})
    if nested_cfg:
        for suffix, ncfg in nested_cfg.items():
            top_field = ncfg["json_path"].split(".")[1].split("[")[0]
            df = pl.from_dicts(rows)

            if top_field not in df.columns:
                continue

            exploded = []
            for r in rows:
                parent_id = r.get(getattr(spec, "RAW_ID_FIELD", "id")) or r.get("id")
                arr = r.get(top_field) or []

                if not isinstance(arr, list):
                    continue

                for elem in arr:
                    if not isinstance(elem, dict):
                        continue

                    new_rec = {ncfg["field_map"].get(k, k): v for k, v in elem.items()}
                    new_rec[getattr(spec, "ID_ALIAS", spec.KEY_COLUMNS[0])] = parent_id
                    exploded.append(new_rec)

            if exploded:
                nest_df = pl.from_dicts(exploded).to_pandas()
                write_data(
                    nest_df,
                    f"{intermediate_schema}.{data_type}_{suffix}",
                    key_columns=ncfg["key_columns"],
                    if_exists="truncate",
                )
                logger.info(
                    f"   ↳ nested table {intermediate_schema}.{data_type}_{suffix} ← {len(nest_df)} rows"
                )
                nested_tables += 1
                total_nested_rows += len(nest_df)

    return {
        "nested_tables": nested_tables,
        "total_nested_rows": total_nested_rows,
    }


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------


def main():
    data_type = os.environ.get("DATA_TYPE", "bills")
    if data_type not in RAW_SCHEMA_MAP:
        raise ValueError(f"No RAW_SCHEMA_MAP entry for '{data_type}'")
    normalize_data_type(data_type)


if __name__ == "__main__":
    main()
