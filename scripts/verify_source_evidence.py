"""Verify a BICAM content-addressed source-evidence bundle offline."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
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
    if path.parent != bundle_dir / prefix.rstrip("/") or path.is_symlink() or not path.is_file():
        raise ValueError(f"source-evidence file is missing or symlinked: {relative_name!r}")
    return path


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
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError("source-evidence manifest is not valid JSON") from error
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise ValueError("unsupported source-evidence manifest schema")
    if manifest.get("bundle_type") != "bicam-source-evidence":
        raise ValueError("unexpected source-evidence bundle type")
    payload_entries = manifest.get("payloads")
    observations = manifest.get("observations")
    if not isinstance(payload_entries, list) or not isinstance(observations, list):
        raise ValueError("source-evidence manifest has no payload or observation list")
    expected_payloads: dict[str, dict[str, Any]] = {}
    for entry in payload_entries:
        if not isinstance(entry, dict):
            raise ValueError("source-evidence payload entry is malformed")
        digest = entry.get("sha256")
        file_name = entry.get("file")
        if not isinstance(digest, str) or len(digest) != 64 or not isinstance(file_name, str):
            raise ValueError("source-evidence payload entry has an invalid digest or filename")
        if digest in expected_payloads or file_name != f"payloads/{digest}.json":
            raise ValueError("source-evidence payload inventory is duplicated or inconsistent")
        path = _safe_relative_file(bundle_dir, file_name, prefix="payloads/")
        size, actual_digest = _hash_file(path)
        if actual_digest != digest or size != entry.get("bytes"):
            raise ValueError(f"source-evidence payload bytes differ: {file_name}")
        expected_payloads[digest] = entry
    payload_dir = bundle_dir / "payloads"
    actual_payloads = {
        path.relative_to(bundle_dir).as_posix()
        for path in payload_dir.glob("*")
        if path.is_file() or path.is_symlink()
    } if payload_dir.is_dir() else set()
    if not payload_dir.is_dir() or any(path.is_dir() for path in payload_dir.iterdir()):
        raise ValueError("source-evidence payload directory contains undeclared directories")
    if actual_payloads != {str(entry["file"]) for entry in payload_entries}:
        raise ValueError("source-evidence payload directory contains undeclared files")
    referenced: set[str] = set()
    for observation in observations:
        if not isinstance(observation, dict):
            raise ValueError("source-evidence observation is malformed")
        payload = observation.get("payload")
        if not isinstance(payload, dict) or payload.get("sha256") not in expected_payloads:
            raise ValueError("source-evidence observation references an unknown payload")
        referenced.add(str(payload["sha256"]))
    if manifest.get("observation_count") != len(observations):
        raise ValueError("source-evidence observation count is inconsistent")
    if manifest.get("unique_payload_count") != len(expected_payloads) or referenced != set(expected_payloads):
        raise ValueError("source-evidence payload counts or references are inconsistent")
    actual_files = {
        path.relative_to(bundle_dir).as_posix()
        for path in bundle_dir.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    expected_files = {"manifest.json", "manifest.sha256"} | {str(entry["file"]) for entry in payload_entries}
    if actual_files != expected_files:
        raise ValueError("source-evidence bundle contains undeclared files")
    return {
        "valid": True,
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
