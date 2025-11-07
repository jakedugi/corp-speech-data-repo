#!/usr/bin/env python3
"""
Validate the complete data pipeline end-to-end.

This script tests that:
1. All imports work correctly
2. Deterministic ID generation works
3. Fixture data is valid
4. Manifest generation works
5. Basic CLI functionality works
"""

import hashlib
import json
import pathlib
import sys

from corpus_types.utils.deterministic_ids import (
    generate_doc_id,
    generate_quote_id,
    validate_id_uniqueness,
)


def test_imports():
    """Test that all critical imports work."""
    print("Testing imports...")

    try:
        from corpus_types.schemas.models import Doc, Outcome, Quote

        print("OK corpus_types imports work")
    except ImportError as e:
        print(f"ERROR corpus_types import failed: {e}")
        return False

    try:
        from corpus_api.client.base_api_client import BaseAPIClient

        print("OK corpus_api imports work")
    except ImportError as e:
        print(f"ERROR corpus_api import failed: {e}")
        return False

    try:
        from corpus_cleaner.cleaner import TextCleaner

        print("OK corpus_cleaner imports work")
    except ImportError as e:
        print(f"ERROR corpus_cleaner import failed: {e}")
        return False

    return True


def test_deterministic_ids():
    """Test deterministic ID generation."""
    print("\n🔢 Testing deterministic ID generation...")

    # Test quote ID generation
    id1 = generate_quote_id("doc_test", 10, 20, "test quote")
    id2 = generate_quote_id("doc_test", 10, 20, "test quote")
    if id1 == id2:
        print(f"OK Quote ID generation deterministic: {id1}")
    else:
        print(f"ERROR Quote ID generation not deterministic: {id1} != {id2}")
        return False

    # Test doc ID generation
    doc_id1 = generate_doc_id("https://example.com/test", "2024-01-01T12:00:00Z")
    doc_id2 = generate_doc_id("https://example.com/test", "2024-01-01T12:00:00Z")
    if doc_id1 == doc_id2:
        print(f"OK Doc ID generation deterministic: {doc_id1}")
    else:
        print(f"ERROR Doc ID generation not deterministic: {doc_id1} != {doc_id2}")
        return False

    return True


def test_fixture_integrity():
    """Test fixture data integrity."""
    print("\nTesting fixture data integrity...")

    fixtures_dir = pathlib.Path("corpus_types/fixtures")

    # Test docs fixture
    docs_file = fixtures_dir / "docs.raw.small.jsonl"
    if not docs_file.exists():
        print(f"ERROR Docs fixture missing: {docs_file}")
        return False

    docs = []
    with docs_file.open("r") as f:
        for line in f:
            if line.strip():
                docs.append(json.loads(line.strip()))

    print(f"OK Loaded {len(docs)} documents from fixture")

    # Validate docs have required fields
    for i, doc in enumerate(docs):
        required_fields = ["doc_id", "raw_text", "schema_version"]
        for field in required_fields:
            if field not in doc:
                print(f"ERROR Document {i} missing field: {field}")
                return False

    # Test quotes fixture
    quotes_file = fixtures_dir / "quotes.small.jsonl"
    if not quotes_file.exists():
        print(f"ERROR Quotes fixture missing: {quotes_file}")
        return False

    quotes = []
    with quotes_file.open("r") as f:
        for line in f:
            if line.strip():
                quotes.append(json.loads(line.strip()))

    print(f"OK Loaded {len(quotes)} quotes from fixture")

    # Test ID references
    doc_ids = {doc["doc_id"] for doc in docs}
    for quote in quotes:
        if quote["doc_id"] not in doc_ids:
            print(f"ERROR Quote references non-existent doc_id: {quote['doc_id']}")
            return False

    print("OK All quote doc_id references are valid")

    # Test ID uniqueness
    doc_ids_list = [doc["doc_id"] for doc in docs]
    is_unique, duplicates = validate_id_uniqueness(
        [{"id": id} for id in doc_ids_list], "id"
    )
    if not is_unique:
        print(f"ERROR Duplicate document IDs: {duplicates}")
        return False

    quote_ids_list = [quote["quote_id"] for quote in quotes]
    is_unique, duplicates = validate_id_uniqueness(
        [{"id": id} for id in quote_ids_list], "id"
    )
    if not is_unique:
        print(f"ERROR Duplicate quote IDs: {duplicates}")
        return False

    print("OK All IDs are unique")
    return True


def test_manifest_generation():
    """Test manifest generation."""
    print("\nTesting manifest generation...")

    fixtures_dir = pathlib.Path("corpus_types/fixtures")

    # Generate manifest
    import subprocess

    result = subprocess.run(
        [sys.executable, "scripts/write_manifest.py", str(fixtures_dir)],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"ERROR Manifest generation failed: {result.stderr}")
        return False

    # Check manifest exists
    manifest_file = fixtures_dir / "manifest.json"
    if not manifest_file.exists():
        print(f"ERROR Manifest file not created: {manifest_file}")
        return False

    # Validate manifest content
    with manifest_file.open("r") as f:
        manifest = json.load(f)

    required_keys = ["generated_at", "versions", "artifacts", "counts", "fingerprints"]
    for key in required_keys:
        if key not in manifest:
            print(f"ERROR Manifest missing key: {key}")
            return False

    print("OK Manifest generated successfully")
    print(f"📊 Artifacts: {manifest['artifacts']}")
    print(f"📈 Counts: {manifest['counts']}")
    return True


def test_text_cleaner():
    """Test text cleaner functionality."""
    print("\n🧽 Testing text cleaner...")

    try:
        from corpus_cleaner.cleaner import TextCleaner

        cleaner = TextCleaner()

        test_text = "Hello   world\n\nwith  extra    spaces."
        cleaned = cleaner.clean(test_text)

        if "Hello world" in cleaned and "\n\n" in cleaned:
            print("OK Text cleaner works correctly")
            return True
        else:
            print(f"ERROR Text cleaner output unexpected: {cleaned}")
            return False
    except Exception as e:
        print(f"ERROR Text cleaner test failed: {e}")
        return False


def main():
    """Run all validation tests."""
    print("Data Pipeline Validation")
    print("=" * 40)

    tests = [
        ("Imports", test_imports),
        ("Deterministic IDs", test_deterministic_ids),
        ("Fixture Integrity", test_fixture_integrity),
        ("Manifest Generation", test_manifest_generation),
        ("Text Cleaner", test_text_cleaner),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"ERROR {test_name} failed")
        except Exception as e:
            print(f"ERROR {test_name} crashed: {e}")

    print("\n" + "=" * 40)
    print(f"Results: {passed}/{total} tests passed")

    if passed == total:
        print("All tests passed! Data pipeline is ready.")
        return 0
    else:
        print("WARNING: Some tests failed. Check the output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
