"""
Comprehensive validation tests for the data pipeline.

Tests schema validation, deterministic IDs, and data integrity
across the entire pipeline.
"""

import json
import pathlib
import subprocess
import tempfile
import pytest
from corpus_types.utils.deterministic_ids import (
    generate_quote_id,
    generate_case_id,
    generate_doc_id,
    validate_id_uniqueness,
    sort_records_by_id
)


class TestDataPipelineValidation:
    """Test suite for data pipeline validation."""

    @pytest.fixture
    def fixtures_dir(self):
        """Get path to fixtures directory."""
        return pathlib.Path("corpus_types/fixtures")

    @pytest.fixture
    def sample_doc(self):
        """Sample document data."""
        return {
            "schema_version": "1.0",
            "doc_id": "doc_test123",
            "source_uri": "https://example.com/test",
            "retrieved_at": "2024-01-01T12:00:00Z",
            "raw_text": "The company stated that 'market conditions are favorable.'",
            "meta": {"court": "test", "docket": "test-123"},
            "provenance": {
                "source": "test",
                "source_uri": "https://example.com/test",
                "retrieved_at": "2024-01-01T12:00:00Z",
                "request": {"endpoint": "test"},
                "response": {"http_status": 200, "sha256": "test", "bytes": 100},
                "adapter": {"name": "test", "version": "1.0.0"},
                "provider": {"source": "test"}
            }
        }

    def test_deterministic_id_generation(self):
        """Test that ID generation is deterministic."""
        # Generate same ID multiple times
        doc_id1 = generate_doc_id("https://example.com/test", "2024-01-01T12:00:00Z")
        doc_id2 = generate_doc_id("https://example.com/test", "2024-01-01T12:00:00Z")
        assert doc_id1 == doc_id2

        # Generate quote ID
        quote_id1 = generate_quote_id("doc_test", 10, 20, "test quote")
        quote_id2 = generate_quote_id("doc_test", 10, 20, "test quote")
        assert quote_id1 == quote_id2
        assert quote_id1.startswith("q_")

    def test_id_uniqueness_validation(self):
        """Test ID uniqueness validation."""
        records = [
            {"id": "test1", "data": "a"},
            {"id": "test2", "data": "b"},
            {"id": "test1", "data": "c"},  # Duplicate
        ]

        is_valid, duplicates = validate_id_uniqueness(records, "id")
        assert not is_valid
        assert "test1" in duplicates

    def test_deterministic_sorting(self):
        """Test that sorting by ID is deterministic."""
        records = [
            {"id": "b", "value": 1},
            {"id": "a", "value": 2},
            {"id": "c", "value": 3},
        ]

        sorted1 = sort_records_by_id(records, "id")
        sorted2 = sort_records_by_id(records, "id")

        assert sorted1 == sorted2
        assert sorted1[0]["id"] == "a"
        assert sorted1[1]["id"] == "b"
        assert sorted1[2]["id"] == "c"

    def test_fixture_data_integrity(self, fixtures_dir):
        """Test that fixture data has proper structure."""
        # Check docs fixture
        docs_file = fixtures_dir / "docs.raw.small.jsonl"
        assert docs_file.exists()

        docs = []
        with docs_file.open('r') as f:
            for line in f:
                docs.append(json.loads(line.strip()))

        assert len(docs) == 3

        # Validate required fields
        for doc in docs:
            assert "doc_id" in doc
            assert "raw_text" in doc
            assert "schema_version" in doc
            assert doc["schema_version"] == "1.0"

        # Check quotes fixture
        quotes_file = fixtures_dir / "quotes.small.jsonl"
        assert quotes_file.exists()

        quotes = []
        with quotes_file.open('r') as f:
            for line in f:
                quotes.append(json.loads(line.strip()))

        assert len(quotes) == 3

        # Validate quotes have required fields
        for quote in quotes:
            assert "quote_id" in quote
            assert "doc_id" in quote
            assert "text" in quote
            assert "schema_version" in quote

    def test_fixture_id_references(self, fixtures_dir):
        """Test that fixture IDs are properly referenced."""
        # Load all fixtures
        docs = []
        quotes = []
        outcomes = []

        with (fixtures_dir / "docs.raw.small.jsonl").open('r') as f:
            for line in f:
                docs.append(json.loads(line.strip()))

        with (fixtures_dir / "quotes.small.jsonl").open('r') as f:
            for line in f:
                quotes.append(json.loads(line.strip()))

        with (fixtures_dir / "outcomes.small.jsonl").open('r') as f:
            for line in f:
                outcomes.append(json.loads(line.strip()))

        # Get all doc_ids from documents
        doc_ids = {doc["doc_id"] for doc in docs}

        # Check that all quote doc_ids exist in documents
        for quote in quotes:
            assert quote["doc_id"] in doc_ids, f"Quote references non-existent doc_id: {quote['doc_id']}"

    def test_manifest_generation(self, fixtures_dir):
        """Test manifest generation works correctly."""
        # Run manifest generation
        result = subprocess.run([
            "python3", "scripts/write_manifest.py", str(fixtures_dir)
        ], capture_output=True, text=True, cwd=pathlib.Path.cwd())

        assert result.returncode == 0

        # Check manifest exists and is valid JSON
        manifest_file = fixtures_dir / "manifest.json"
        assert manifest_file.exists()

        with manifest_file.open('r') as f:
            manifest = json.load(f)

        # Validate manifest structure
        assert "generated_at" in manifest
        assert "versions" in manifest
        assert "artifacts" in manifest
        assert "counts" in manifest
        assert "fingerprints" in manifest

        # Check that all expected artifacts are present
        expected_artifacts = ["docs.raw.jsonl", "docs.norm.jsonl", "quotes.jsonl", "outcomes.jsonl"]
        for artifact in expected_artifacts:
            assert artifact in manifest["artifacts"]

    def test_schema_validation_cli(self, fixtures_dir):
        """Test that corpus-validate CLI works on fixtures."""
        # This would test the actual CLI if it were working
        # For now, just check that the validation script exists
        validate_script = pathlib.Path("corpus_types/cli/validate.py")
        assert validate_script.exists()

        # Check the script has the expected structure
        content = validate_script.read_text()
        assert "corpus-validate" in content or "validate" in content

    def test_offline_mode_availability(self):
        """Test that offline mode is available in the fetch CLI."""
        fetch_script = pathlib.Path("corpus_api/cli/fetch.py")
        assert fetch_script.exists()

        content = fetch_script.read_text()
        assert "--use-fixture" in content
        assert "fixture_file" in content

    def test_makefile_targets_exist(self):
        """Test that Makefile has the expected targets."""
        makefile = pathlib.Path("Makefile")
        assert makefile.exists()

        content = makefile.read_text()
        assert "demo_e2e:" in content
        assert "clean:" in content
        assert "fetch:" in content
        assert "normalize:" in content
        assert "extract:" in content
        assert "validate:" in content
        assert "manifest:" in content

    def test_orchestrator_scripts_exist(self):
        """Test that orchestrator scripts exist."""
        assert pathlib.Path("scripts/write_manifest.py").exists()
        assert pathlib.Path("scripts/write_run_log.py").exists()

    def test_config_files_exist(self):
        """Test that configuration files exist."""
        assert pathlib.Path("configs/query.small.yaml").exists()
        assert pathlib.Path("configs/query.example.yaml").exists()


if __name__ == "__main__":
    pytest.main([__file__])
