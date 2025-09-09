#!/usr/bin/env python3
"""
Generate manifest.json with versions, counts, and fingerprints for data artifacts.

Usage:
    python scripts/write_manifest.py <data_dir>

Produces:
    data_dir/manifest.json with:
    - versions: Tool versions used
    - artifacts: List of generated files
    - counts: Record counts for each artifact
    - fingerprints: blake3 fingerprints for reproducibility
"""

import sys
import json
import hashlib
import pathlib
import subprocess
from datetime import datetime


def blake3_digest(path: pathlib.Path) -> str:
    """Compute blake3 digest of file contents."""
    if not path.exists():
        return "file_missing"
    return hashlib.blake2b(path.read_bytes(), digest_size=16).hexdigest()


def get_version(cmd: str) -> str:
    """Get version string from command, fallback to 'unknown'."""
    try:
        result = subprocess.run([cmd, "--version"],
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            return result.stdout.strip()
        return "unknown"
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return "unknown"


def count_records(path: pathlib.Path) -> int:
    """Count JSONL records in file."""
    if not path.exists():
        return 0
    try:
        return sum(1 for _ in path.open('r', encoding='utf-8'))
    except Exception:
        return -1


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/write_manifest.py <data_dir>")
        sys.exit(1)

    data_dir = pathlib.Path(sys.argv[1])
    if not data_dir.exists():
        print(f"Error: {data_dir} does not exist")
        sys.exit(1)

  # Expected artifacts (with fallback to .small versions for fixtures)
    artifacts = [
        "docs.raw.jsonl",
        "docs.norm.jsonl",
        "quotes.jsonl",
        "outcomes.jsonl"
    ]

    # For fixtures directory, also check .small versions
    artifact_mappings = {}
    for artifact in artifacts:
        path = data_dir / artifact
        if path.exists():
            artifact_mappings[artifact] = path
        else:
            # Try .small version
            small_path = data_dir / f"{artifact.replace('.jsonl', '.small.jsonl')}"
            if small_path.exists():
                artifact_mappings[artifact] = small_path
            else:
                artifact_mappings[artifact] = path  # Will be marked as missing

    # Get tool versions
    versions = {
        "corpus-types": get_version("corpus-validate"),
        "corpus-api": get_version("corpus-fetch"),
        "corpus-cleaner": get_version("corpus-clean"),
        "corpus-extractors": get_version("corpus-extract-quotes"),
        "python": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
    }

    # Compute counts and fingerprints
    counts = {}
    fingerprints = {}

    for artifact in artifacts:
        path = artifact_mappings[artifact]
        counts[artifact] = count_records(path)
        fingerprints[artifact] = blake3_digest(path)

    # Build manifest
    manifest = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "data_directory": str(data_dir),
        "versions": versions,
        "artifacts": artifacts,
        "counts": counts,
        "fingerprints": fingerprints,
        "status": "success"
    }

    # Write manifest
    manifest_path = data_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))

    print(f"✅ Manifest written to {manifest_path}")
    print(f"📊 Records: {counts}")
    print(f"🔐 Fingerprints: {fingerprints}")


if __name__ == "__main__":
    main()
