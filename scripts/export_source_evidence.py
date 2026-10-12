"""Write a content-addressed bundle of selected raw_response_versions payloads.

Endpoint metadata is sanitized but payloads are not: check them for echoed
credentials before distributing.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg

from bicam_collection.config import Settings


_SECRET_QUERY_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "access_token",
        "auth",
        "key",
        "token",
    }
)


def sanitize_endpoint(url: str) -> str:
    """Remove credential-like query values while retaining endpoint identity."""

    parts = urlsplit(url)
    query: list[tuple[str, str]] = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        query.append((key, "[redacted]" if key.lower() in _SECRET_QUERY_KEYS else value))
    return urlunsplit((parts.scheme, parts.netloc.rsplit("@", 1)[-1], parts.path, urlencode(sorted(query)), ""))


def canonical_payload_bytes(payload: Any) -> bytes:
    """Serialize JSON deterministically for hashing and offline verification."""

    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        .encode("utf-8")
        + b"\n"
    )


def _text(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else str(value) if value is not None else None


def _record_from_row(row: Mapping[str, Any] | Sequence[Any]) -> dict[str, Any]:
    """Normalize a database row for the bundle writer and tests."""

    if isinstance(row, Mapping):
        get = row.__getitem__
    else:
        names = (
            "raw_response_version_id",
            "raw_response_id",
            "endpoint_url",
            "source",
            "category",
            "node_type",
            "parent_id",
            "response_hash",
            "first_seen_at",
            "last_seen_at",
            "first_crawl_run_id",
            "latest_crawl_run_id",
            "response",
        )
        if len(row) != len(names):
            raise ValueError(f"source evidence row has {len(row)} values; expected {len(names)}")
        get = dict(zip(names, row, strict=True)).__getitem__
    return {
        "raw_response_version_id": _text(get("raw_response_version_id")),
        "raw_response_id": _text(get("raw_response_id")),
        "endpoint": sanitize_endpoint(str(get("endpoint_url"))),
        "source": str(get("source")),
        "category": str(get("category")),
        "node_type": str(get("node_type")),
        "parent_id": _text(get("parent_id")),
        "source_response_hash": str(get("response_hash")),
        "first_seen_at": _text(get("first_seen_at")),
        "last_seen_at": _text(get("last_seen_at")),
        "first_crawl_run_id": _text(get("first_crawl_run_id")),
        "latest_crawl_run_id": _text(get("latest_crawl_run_id")),
        "response": get("response"),
    }


def write_bundle(
    rows: Iterable[Mapping[str, Any] | Sequence[Any]],
    output_dir: Path,
    *,
    selection: Mapping[str, Any],
    generated_at: datetime | None = None,
    database_snapshot_at: datetime | None = None,
) -> dict[str, Any]:
    """Write rows into a new immutable bundle and return its manifest."""

    generated_at = (generated_at or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0)
    output_dir.mkdir(parents=True, exist_ok=True)
    name = generated_at.strftime("%Y%m%dT%H%M%SZ")
    final_dir = output_dir / name
    if final_dir.exists():
        raise FileExistsError(f"source evidence bundle already exists: {final_dir}")
    temporary_dir = Path(tempfile.mkdtemp(prefix=".source-evidence-", dir=output_dir))
    try:
        payload_dir = temporary_dir / "payloads"
        payload_dir.mkdir()
        observations: list[dict[str, Any]] = []
        payload_files: dict[str, dict[str, Any]] = {}
        for raw_row in rows:
            record = _record_from_row(raw_row)
            payload = canonical_payload_bytes(record.pop("response"))
            digest = hashlib.sha256(payload).hexdigest()
            file_name = f"payloads/{digest}.json"
            file_path = temporary_dir / file_name
            if digest not in payload_files:
                file_path.write_bytes(payload)
                payload_files[digest] = {
                    "file": file_name,
                    "bytes": len(payload),
                    "sha256": digest,
                }
            record["payload"] = payload_files[digest]
            observations.append(record)
        manifest = {
            "schema_version": 1,
            "bundle_type": "bicam-source-evidence",
            "reproduction_level": "content-addressed source-payload subset",
            "generated_at": generated_at.isoformat(),
            "database_snapshot_at": database_snapshot_at.isoformat() if database_snapshot_at else None,
            "selection": dict(selection),
            "observation_count": len(observations),
            "unique_payload_count": len(payload_files),
            "payloads": [payload_files[digest] for digest in sorted(payload_files)],
            "observations": observations,
        }
        if manifest["selection"].get("endpoint_prefix"):
            manifest["selection"]["endpoint_prefix"] = sanitize_endpoint(manifest["selection"]["endpoint_prefix"])
        manifest_path = temporary_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        # The digest belongs in a sidecar, never in the body it hashes.
        manifest_digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        (temporary_dir / "manifest.sha256").write_text(f"{manifest_digest}  manifest.json\n", encoding="ascii")
        temporary_dir.rename(final_dir)
        return {
            **manifest,
            "manifest_sha256": manifest_digest,
            "manifest_path": str(final_dir / "manifest.json"),
            "bundle_dir": str(final_dir),
        }
    except Exception:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise


def _query_rows(
    connection: psycopg.Connection,
    *,
    limit: int,
    source: str | None,
    category: str | None,
    node_type: str | None,
    endpoint_prefix: str | None,
) -> list[tuple[Any, ...]]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    predicates = ["true"]
    params: list[Any] = []
    for column, value in (("source", source), ("category", category), ("node_type", node_type)):
        if value:
            predicates.append(f"{column} = %s")
            params.append(value)
    if endpoint_prefix:
        predicates.append("endpoint_url like %s")
        params.append(endpoint_prefix + "%")
    params.append(limit)
    return connection.execute(
        """
        select id, raw_response_id, endpoint_url, source, category, node_type,
               parent_id, response_hash, first_seen_at, last_seen_at,
               first_crawl_run_id, latest_crawl_run_id, response
        from raw_response_versions
        where """
        + " and ".join(predicates)
        + " order by last_seen_at desc, id\n"
        + "limit %s",
        params,
    ).fetchall()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("../reports/source-evidence"))
    parser.add_argument("--limit", type=int, default=100, help="Maximum immutable payload observations (default: 100).")
    parser.add_argument("--source")
    parser.add_argument("--category")
    parser.add_argument("--node-type")
    parser.add_argument("--endpoint-prefix")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    selection = {
        "source": args.source,
        "category": args.category,
        "node_type": args.node_type,
        "endpoint_prefix": args.endpoint_prefix,
        "limit": args.limit,
    }
    with psycopg.connect(settings.database_url) as connection:
        connection.execute("set transaction isolation level repeatable read")
        connection.execute("set transaction read only")
        snapshot_at = connection.execute("select transaction_timestamp()").fetchone()[0]
        rows = _query_rows(
            connection,
            limit=args.limit,
            source=args.source,
            category=args.category,
            node_type=args.node_type,
            endpoint_prefix=args.endpoint_prefix,
        )
    manifest = write_bundle(rows, args.output_dir, selection=selection, database_snapshot_at=snapshot_at)
    print(
        f"wrote source evidence bundle observations={manifest['observation_count']} "
        f"payloads={manifest['unique_payload_count']} path={manifest['bundle_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
