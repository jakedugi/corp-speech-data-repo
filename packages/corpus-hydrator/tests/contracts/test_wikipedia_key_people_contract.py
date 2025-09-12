"""
Contract Tests for Wikipedia Key People Scraper

These tests ensure all components satisfy the same contracts and invariants,
guaranteeing consistent behavior across different implementations.
"""

import pytest
import json
import unicodedata
from pathlib import Path
from typing import Dict, Any, List
from unittest.mock import Mock, patch

# Import the components we want to test
from corpus_hydrator.adapters.wikipedia_key_people.normalize import WikipediaKeyPeopleNormalizer
from corpus_hydrator.adapters.wikipedia_key_people.core.scraper import WikipediaKeyPeopleScraper
from corpus_hydrator.adapters.wikipedia_key_people.writer import WikipediaKeyPeopleWriter
from corpus_types.schemas.wikipedia_key_people import (
    WikipediaKeyPerson,
    NormalizedCompany,
    NormalizedPerson,
    NormalizedRole,
    NormalizedAppointment
)


class TestWikipediaKeyPeopleContracts:
    """Test contracts that all components must satisfy."""

    @pytest.fixture
    def normalizer(self):
        """Normalizer instance."""
        return WikipediaKeyPeopleNormalizer()

    @pytest.fixture
    def writer(self):
        """Writer instance."""
        return WikipediaKeyPeopleWriter()

    @pytest.fixture
    def sample_person(self):
        """Sample person for testing."""
        return WikipediaKeyPerson(
            ticker="AAPL",
            company_name="Apple Inc.",
            raw_name="Tim Cook",
            clean_name="Tim Cook",
            clean_title="Chief Executive Officer",
            source="wikipedia",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            extraction_method="infobox",
            scraped_at=None,
            parse_success=True,
            confidence_score=0.95
        )

    def test_normalizer_interface_contract(self, normalizer):
        """Test that normalizer implements required interface."""
        # Normalizer must have normalization methods
        required_methods = [
            'normalize_name',
            'normalize_title',
            'normalize_people',
            'validate_people_data',
            'normalize_name_unicode',  # v2.0
            'normalize_title_controlled_vocabulary',  # v2.0
            'deduplicate_people_advanced',  # v2.0
            'normalize_people_batch'  # v2.0
        ]

        for method_name in required_methods:
            assert hasattr(normalizer, method_name), f"Normalizer missing method: {method_name}"
            assert callable(getattr(normalizer, method_name)), f"Normalizer method not callable: {method_name}"

    def test_writer_interface_contract(self, writer):
        """Test that writer implements required interface."""
        # Writer must have writing methods
        required_methods = [
            'write_people_to_csv',
            'write_comparison_report',
            'convert_legacy_to_normalized',
            'write_normalized_tables',
            'write_deterministic_csv',  # v2.0
            'write_deterministic_parquet',  # v2.0
            'generate_dataset_manifest',  # v2.0
            'write_normalized_tables_with_manifest'  # v2.0
        ]

        for method_name in required_methods:
            assert hasattr(writer, method_name), f"Writer missing method: {method_name}"
            assert callable(getattr(writer, method_name)), f"Writer method not callable: {method_name}"

    def test_normalization_contract(self, normalizer, sample_person):
        """Test that normalization produces consistent results."""
        # Test Unicode normalization
        unicode_name = "José María González"
        normalized_unicode = normalizer.normalize_name_unicode(unicode_name)
        assert normalized_unicode == unicodedata.normalize('NFC', unicode_name)

        # Test controlled vocabulary
        title_variations = [
            "Chief Executive Officer",
            "CEO",
            "Chief Exec",
            "Executive Director"
        ]

        for title in title_variations:
            normalized = normalizer.normalize_title_controlled_vocabulary(title)
            # All CEO variations should normalize to the same canonical form
            if any(ceo_term in title.upper() for ceo_term in ["CEO", "CHIEF EXEC", "EXECUTIVE DIRECTOR"]):
                assert normalized.upper() in ["CHIEF EXECUTIVE OFFICER", "PRESIDENT"]

    def test_deduplication_contract(self, normalizer):
        """Test that deduplication works correctly."""
        # Create duplicate people
        person1 = WikipediaKeyPerson(
            ticker="AAPL",
            company_name="Apple Inc.",
            raw_name="Tim Cook",
            clean_name="Tim Cook",
            clean_title="CEO",
            source="wikipedia",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            extraction_method="infobox",
            confidence_score=0.9
        )

        person2 = WikipediaKeyPerson(
            ticker="AAPL",
            company_name="Apple Inc.",
            raw_name="Timothy Cook",
            clean_name="Timothy Cook",
            clean_title="Chief Executive Officer",
            source="wikipedia",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            extraction_method="section",
            confidence_score=0.95
        )

        duplicates = [person1, person2]
        deduplicated = normalizer.deduplicate_people_advanced(duplicates)

        # Should deduplicate to one person
        assert len(deduplicated) == 1

        # Should keep the higher confidence score person
        winner = deduplicated[0]
        assert winner.confidence_score == 0.95
        assert winner.extraction_method == "section"

    def test_batch_normalization_contract(self, normalizer):
        """Test that batch normalization preserves data integrity."""
        people = [
            WikipediaKeyPerson(
                ticker="AAPL",
                company_name="Apple Inc.",
                raw_name="Tim Cook",
                clean_name="Tim Cook",
                clean_title="CEO",
                source="wikipedia",
                wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
                extraction_method="infobox"
            ),
            WikipediaKeyPerson(
                ticker="MSFT",
                company_name="Microsoft Corporation",
                raw_name="Satya Nadella",
                clean_name="Satya Nadella",
                clean_title="CEO",
                source="wikipedia",
                wikipedia_url="https://en.wikipedia.org/wiki/Microsoft",
                extraction_method="infobox"
            )
        ]

        original_count = len(people)
        normalized = normalizer.normalize_people_batch(people, deduplicate=False)

        # Should preserve count when not deduplicating
        assert len(normalized) == original_count

        # Should apply Unicode normalization
        for person in normalized:
            assert person.clean_name == normalizer.normalize_name_unicode(person.clean_name)

    def test_deterministic_output_contract(self, writer, tmp_path):
        """Test that deterministic output produces consistent results."""
        companies = [
            {"company_id": "B", "company_name": "Company B", "ticker": "COMP_B"},
            {"company_id": "A", "company_name": "Company A", "ticker": "COMP_A"}
        ]

        # Write twice
        file1 = tmp_path / "test1.csv"
        file2 = tmp_path / "test2.csv"

        hash1 = writer.write_deterministic_csv(companies, file1, ["company_id"])
        hash2 = writer.write_deterministic_csv(companies, file2, ["company_id"])

        # Should produce identical hashes
        assert hash1 == hash2

        # Should produce identical content
        with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
            assert f1.read() == f2.read()

    def test_manifest_generation_contract(self, writer):
        """Test that manifest generation follows schema."""
        output_files = {"companies_csv": "abc123", "people_csv": "def456"}
        metadata = {
            "row_counts": {"companies": 10, "people": 25},
            "provider_order": ["wikipedia"],
            "extraction_parameters": {"workers": 2}
        }

        manifest = writer.generate_dataset_manifest(
            "test_dataset",
            output_files,
            metadata
        )

        # Manifest must have required fields
        required_fields = [
            "schema_version",
            "dataset_name",
            "extraction_timestamp",
            "row_counts",
            "file_hashes",
            "source_metadata",
            "governance"
        ]

        for field in required_fields:
            assert field in manifest, f"Manifest missing required field: {field}"

        # Schema version should be 2.0.0
        assert manifest["schema_version"] == "2.0.0"

        # Row counts should match input
        assert manifest["row_counts"]["companies"] == 10
        assert manifest["row_counts"]["people"] == 25

    def test_data_validation_contract(self, normalizer):
        """Test that data validation catches common issues."""
        # Valid person
        valid_person = WikipediaKeyPerson(
            ticker="AAPL",
            company_name="Apple Inc.",
            raw_name="Tim Cook",
            clean_name="Tim Cook",
            clean_title="CEO",
            source="wikipedia",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            extraction_method="infobox"
        )

        # Invalid person (empty name)
        invalid_person = WikipediaKeyPerson(
            ticker="AAPL",
            company_name="Apple Inc.",
            raw_name="",
            clean_name="",
            clean_title="CEO",
            source="wikipedia",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            extraction_method="infobox"
        )

        people_list = [valid_person, invalid_person]

        # Validation should work without throwing exceptions
        validation_result = normalizer.validate_people_data(people_list)

        # Should have validation results
        assert isinstance(validation_result, dict)
        assert "total_people" in validation_result
        assert "valid_people" in validation_result
        assert validation_result["total_people"] == 2

    def test_error_handling_contract(self, normalizer):
        """Test that components handle errors gracefully."""
        # Test with None input
        result = normalizer.normalize_people_batch(None)
        assert result == []

        # Test with empty list
        result = normalizer.normalize_people_batch([])
        assert result == []

        # Test normalization with problematic strings
        problematic_name = "Name with \x00 null bytes"
        normalized = normalizer.normalize_name_unicode(problematic_name)
        assert isinstance(normalized, str)
        assert len(normalized) > 0

    def test_unicode_handling_contract(self, normalizer):
        """Test that Unicode handling works correctly."""
        test_cases = [
            ("José María", "José María"),  # Already NFC
            ("José María", "José María"),  # NFD to NFC
            ("François Müller", "François Müller"),
            ("Björk Guðmundsdóttir", "Björk Guðmundsdóttir"),
        ]

        for input_name, expected in test_cases:
            # Convert to different normalization forms to test
            nfd_name = unicodedata.normalize('NFD', input_name)
            nfkd_name = unicodedata.normalize('NFKD', input_name)

            # All should normalize to the same NFC result
            assert normalizer.normalize_name_unicode(nfd_name) == normalizer.normalize_name_unicode(expected)
            assert normalizer.normalize_name_unicode(nfkd_name) == normalizer.normalize_name_unicode(expected)

    @pytest.mark.parametrize("sort_keys", [
        ["company_id"],
        ["company_id", "person_id"],
        ["company_id", "person_id", "role_id"]
    ])
    def test_sorting_contract(self, writer, tmp_path, sort_keys):
        """Test that sorting works with different key combinations."""
        data = [
            {"company_id": "B", "person_id": "2", "role_id": "Y", "name": "Person B2"},
            {"company_id": "A", "person_id": "1", "role_id": "Z", "name": "Person A1"},
            {"company_id": "B", "person_id": "1", "role_id": "X", "name": "Person B1"},
        ]

        output_file = tmp_path / "sorted_test.csv"
        writer.write_deterministic_csv(data, output_file, sort_keys)

        # File should exist and be readable
        assert output_file.exists()

        # Read back and verify it's sorted
        import pandas as pd
        df = pd.read_csv(output_file)

        # Verify sorting by checking the order
        for i in range(len(df) - 1):
            current = tuple(df.loc[i, key] for key in sort_keys if key in df.columns)
            next_val = tuple(df.loc[i + 1, key] for key in sort_keys if key in df.columns)

            # Current should be <= next (stable sort)
            assert current <= next_val, f"Sorting failed at row {i}: {current} > {next_val}"


class TestGoldenManifests:
    """Test against golden manifest files for regression detection."""

    @pytest.fixture
    def golden_manifests_dir(self):
        """Directory containing golden manifest files."""
        return Path(__file__).parent / "golden"

    def test_manifest_schema_contract(self, writer):
        """Test that generated manifests conform to expected schema."""
        # This would load golden manifest files and compare against schema
        # For now, just test that manifest generation doesn't crash
        manifest = writer.generate_dataset_manifest(
            "test_dataset",
            {"csv": "testhash"},
            {"row_counts": {"companies": 1}}
        )

        # Should be valid JSON-serializable
        json_str = json.dumps(manifest, default=str)
        parsed_back = json.loads(json_str)

        assert parsed_back["dataset_name"] == "test_dataset"
        assert parsed_back["schema_version"] == "2.0.0"
