"""Verify a BICAM content-addressed source-evidence bundle offline."""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from typing import Any


def _hash_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def _safe_relative_file(bundle_dir: Path, relative_name: str, *, prefix: str) -> Path:
    relative = Path(relative_name)
    if relative.is_absolute() or relative.name != relative_name.split("/")[-1] or not relative_name.startswith(prefix):
        raise ValueError(f"unsafe source-evidence path: {relative_name!r}")
    path = bundle_dir / relative
    if path.parent != bundle_dir / prefix.rstrip("/") or path.parent.is_symlink() or path.is_symlink() or not path.is_file():
        raise ValueError(f"source-evidence file is missing or symlinked: {relative_name!r}")
    return path


def _timestamp(value: Any, field: str) -> datetime:
    try:
        result = datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        result = None
    if result is None or result.utcoffset() is None:
        raise ValueError(f"source-evidence {field} must be a timezone-aware timestamp")
    return result


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"source-evidence manifest repeats JSON key {key!r}")
        result[key] = value
    return result


def verify_bundle(bundle_dir: Path) -> dict[str, Any]:
    """Verify manifest, payload hashes, references, and exact inventory."""

    bundle_dir = bundle_dir.expanduser()
    if not bundle_dir.is_dir() or bundle_dir.is_symlink():
        raise ValueError("source-evidence bundle directory is missing or symlinked")
    manifest_path = bundle_dir / "manifest.json"
    checksum_path = bundle_dir / "manifest.sha256"
    if manifest_path.is_symlink() or not manifest_path.is_file() or checksum_path.is_symlink() or not checksum_path.is_file():
        raise ValueError("source-evidence manifest or checksum sidecar is missing or symlinked")
    checksum_parts = checksum_path.read_text(encoding="ascii").strip().split()
    if len(checksum_parts) != 2 or checksum_parts[1] != "manifest.json":
        raise ValueError("source-evidence manifest checksum sidecar is malformed")
    manifest_size, manifest_digest = _hash_file(manifest_path)
    if checksum_parts[0] != manifest_digest:
        raise ValueError("source-evidence manifest checksum does not match manifest.json")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    except json.JSONDecodeError as error:
        raise ValueError("source-evidence manifest is not valid JSON") from error
    if not isinstance(manifest, dict) or type(manifest.get("schema_version")) is not int or manifest["schema_version"] != 1:
        raise ValueError("unsupported source-evidence manifest schema")
    if manifest.get("bundle_type") != "bicam-source-evidence":
        raise ValueError("unexpected source-evidence bundle type")
    if manifest.get("reproduction_level") != "content-addressed source-payload subset":
        raise ValueError("unsupported source-evidence reproduction level")
    _timestamp(manifest.get("generated_at"), "generated_at")
    snapshot_at = manifest.get("database_snapshot_at")
    if snapshot_at is not None:
        _timestamp(snapshot_at, "database_snapshot_at")
    selection = manifest.get("selection")
    if not isinstance(selection, dict):
        raise ValueError("source-evidence selection must be an object")
    payload_entries = manifest.get("payloads")
    observations = manifest.get("observations")
    if not isinstance(payload_entries, list) or not isinstance(observations, list):
        raise ValueError("source-evidence manifest has no payload or observation list")
    for field, count in (("observation_count", len(observations)), ("unique_payload_count", len(payload_entries))):
        if type(manifest.get(field)) is not int or manifest[field] != count:
            raise ValueError(f"source-evidence {field} is inconsistent")
    if "limit" in selection:
        limit = selection["limit"]
        if type(limit) is not int or limit <= 0 or len(observations) > limit:
            raise ValueError("source-evidence selection limit is inconsistent")
    if {path.name for path in bundle_dir.iterdir()} != {"manifest.json", "manifest.sha256", "payloads"}:
        raise ValueError("source-evidence bundle contains undeclared entries")
    payload_dir = bundle_dir / "payloads"
    if payload_dir.is_symlink() or not payload_dir.is_dir():
        raise ValueError("source-evidence payload directory is missing or symlinked")
    expected_payloads: dict[str, dict[str, Any]] = {}
    for entry in payload_entries:
        if not isinstance(entry, dict):
            raise ValueError("source-evidence payload entry is malformed")
        digest = entry.get("sha256")
        file_name = entry.get("file")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest) or not isinstance(file_name, str):
            raise ValueError("source-evidence payload entry has an invalid digest or filename")
        if type(entry.get("bytes")) is not int or entry["bytes"] < 1:
            raise ValueError("source-evidence payload byte count is invalid")
        if digest in expected_payloads or file_name != f"payloads/{digest}.json":
            raise ValueError("source-evidence payload inventory is duplicated or inconsistent")
        path = _safe_relative_file(bundle_dir, file_name, prefix="payloads/")
        size, actual_digest = _hash_file(path)
        if actual_digest != digest or size != entry.get("bytes"):
            raise ValueError(f"source-evidence payload bytes differ: {file_name}")
        expected_payloads[digest] = entry
    actual_payloads = {
        path.relative_to(bundle_dir).as_posix()
        for path in payload_dir.iterdir()
    }
    if any(not path.is_file() or path.is_symlink() for path in payload_dir.iterdir()):
        raise ValueError("source-evidence payload directory contains undeclared directories")
    if actual_payloads != {str(entry["file"]) for entry in payload_entries}:
        raise ValueError("source-evidence payload directory contains undeclared files")
    referenced: set[str] = set()
    version_ids: set[str] = set()
    for observation in observations:
        if not isinstance(observation, dict):
            raise ValueError("source-evidence observation is malformed")
        payload = observation.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("sha256"), str) or payload["sha256"] not in expected_payloads:
            raise ValueError("source-evidence observation references an unknown payload")
        if payload != expected_payloads[payload["sha256"]]:
            raise ValueError("source-evidence observation payload metadata differs from inventory")
        for field in ("raw_response_version_id", "raw_response_id", "endpoint", "source", "category", "node_type", "source_response_hash"):
            if not isinstance(observation.get(field), str) or not observation[field].strip():
                raise ValueError(f"source-evidence observation is missing {field}")
        version_id = observation["raw_response_version_id"]
        if version_id in version_ids:
            raise ValueError("source-evidence observation duplicates a raw version ID")
        version_ids.add(version_id)
        first_seen = _timestamp(observation.get("first_seen_at"), "first_seen_at")
        last_seen = _timestamp(observation.get("last_seen_at"), "last_seen_at")
        if first_seen > last_seen:
            raise ValueError("source-evidence observation times are reversed")
        for field in ("source", "category", "node_type"):
            if selection.get(field) and selection[field] != observation[field]:
                raise ValueError(f"source-evidence observation does not match selection {field}")
        referenced.add(str(payload["sha256"]))
    if manifest.get("observation_count") != len(observations):
        raise ValueError("source-evidence observation count is inconsistent")
    if manifest.get("unique_payload_count") != len(expected_payloads) or referenced != set(expected_payloads):
        raise ValueError("source-evidence payload counts or references are inconsistent")
    return {
        "valid": True,
        "verification_scope": "bundle integrity and metadata consistency; not public-row reproduction",
        "reproduction_level": manifest["reproduction_level"],
        "database_snapshot_at": snapshot_at,
        "bundle": str(bundle_dir.resolve()),
        "manifest_bytes": manifest_size,
        "manifest_sha256": manifest_digest,
        "observation_count": len(observations),
        "unique_payload_count": len(expected_payloads),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = verify_bundle(args.bundle)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(json.dumps({"valid": False, "error": str(error)}, indent=2, sort_keys=True))
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
