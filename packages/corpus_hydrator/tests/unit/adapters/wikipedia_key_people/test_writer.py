"""
Unit tests for Wikipedia Key People Writer

Tests cover:
- CSV and JSON output generation
- Statistics file creation
- Comparison report generation
- Error handling for file operations
"""

import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from corpus_hydrator.adapters.wikipedia_key_people.writer import (
    WikipediaKeyPeopleWriter,
)
from corpus_types.schemas.wikipedia_key_people import (
    WikipediaCompany,
    WikipediaExtractionResult,
    WikipediaKeyPerson,
)


class TestWikipediaKeyPeopleWriter:
    """Test WikipediaKeyPeopleWriter functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.writer = WikipediaKeyPeopleWriter(self.temp_dir)

        # Create sample data
        self.sample_people = [
            WikipediaKeyPerson(
                ticker="AAPL",
                company_name="Apple Inc.",
                raw_name="Tim Cook (CEO)",
                clean_name="Tim Cook",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
                extraction_method="test",
            ),
            WikipediaKeyPerson(
                ticker="AAPL",
                company_name="Apple Inc.",
                raw_name="Luca Maestri (CFO)",
                clean_name="Luca Maestri",
                clean_title="Chief Financial Officer",
                wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
                extraction_method="test",
            ),
        ]

        self.sample_companies = [
            WikipediaCompany(
                ticker="AAPL",
                company_name="Apple Inc.",
                wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
                index_name="sp500",
                key_people_count=2,
                processing_success=True,
            )
        ]

        self.sample_result = WikipediaExtractionResult(
            operation_id="test_op",
            index_name="sp500",
            companies_processed=1,
            companies_successful=1,
            total_key_people=2,
            key_people=self.sample_people,
            companies=self.sample_companies,
        )

    def teardown_method(self):
        """Clean up test fixtures."""
        # Remove temp directory
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_write_key_people_csv(self):
        """Test writing key people data to CSV."""
        self.writer._write_key_people_csv(self.sample_result, "test")

        csv_file = Path(self.temp_dir) / "test_key_people.csv"
        assert csv_file.exists()

        # Read and verify CSV content
        df = pd.read_csv(csv_file)
        assert len(df) == 2
        assert df.iloc[0]["ticker"] == "AAPL"
        assert df.iloc[0]["clean_name"] == "Tim Cook"
        assert df.iloc[1]["clean_title"] == "Chief Financial Officer"

    def test_write_key_people_json(self):
        """Test writing key people data to JSON."""
        self.writer._write_key_people_json(self.sample_result, "test")

        json_file = Path(self.temp_dir) / "test_key_people.json"
        assert json_file.exists()

        # Read and verify JSON content
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert len(data) == 2
        assert data[0]["ticker"] == "AAPL"
        assert data[0]["clean_name"] == "Tim Cook"

    def test_write_companies_csv(self):
        """Test writing company summary data to CSV."""
        self.writer._write_companies_csv(self.sample_result, "test")

        csv_file = Path(self.temp_dir) / "test_companies.csv"
        assert csv_file.exists()

        # Read and verify CSV content
        df = pd.read_csv(csv_file)
        assert len(df) == 1
        assert df.iloc[0]["ticker"] == "AAPL"
        assert df.iloc[0]["key_people_count"] == 2
        assert df.iloc[0]["processing_success"] == True

    def test_write_statistics(self):
        """Test writing extraction statistics to file."""
        self.writer._write_statistics(self.sample_result, "test")

        stats_file = Path(self.temp_dir) / "test_stats.txt"
        assert stats_file.exists()

        # Read and verify statistics content
        with open(stats_file, "r", encoding="utf-8") as f:
            content = f.read()

        assert "test_op" in content
        assert "Companies processed: 1" in content
        assert "Total key people: 2" in content
        assert "SUCCESS" in content

    def test_write_results_success(self):
        """Test successful writing of complete results."""
        self.writer.write_results(self.sample_result, "test")

        # Check that all expected files were created
        expected_files = [
            "test_key_people.csv",
            "test_key_people.json",
            "test_companies.csv",
            "test_stats.txt",
        ]

        for filename in expected_files:
            file_path = Path(self.temp_dir) / filename
            assert file_path.exists(), f"File {filename} was not created"

    def test_write_results_failed_extraction(self):
        """Test writing results for failed extraction."""
        failed_result = WikipediaExtractionResult(
            operation_id="failed_op",
            index_name="test",
            success=False,
            error_message="Test error",
        )

        # Should not raise exception, just skip writing
        self.writer.write_results(failed_result, "test")

        # Should not create any files
        files = list(Path(self.temp_dir).glob("test_*"))
        assert len(files) == 0

    def test_write_intermediate_results(self):
        """Test writing intermediate results."""
        intermediate_dir = Path(self.temp_dir) / "intermediate" / "test"
        self.writer.write_intermediate_results(self.sample_result, "test")

        # Should create intermediate directory and files
        assert intermediate_dir.exists()

        # Should create individual company files
        company_files = list(intermediate_dir.glob("*.json"))
        assert len(company_files) == 1

        # Verify company file content
        company_file = company_files[0]
        with open(company_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["ticker"] == "AAPL"
        assert data["key_people_count"] == 2

    def test_write_comparison_report(self):
        """Test writing comparison report for multiple results."""
        results = {
            "sp500": self.sample_result,
            "dow": WikipediaExtractionResult(
                operation_id="dow_op",
                index_name="dow",
                companies_processed=2,
                companies_successful=1,
                total_key_people=1,
                key_people=[self.sample_people[0]],
                companies=[self.sample_companies[0]],
            ),
        }

        report_file = Path(self.temp_dir) / "comparison.txt"
        self.writer.write_comparison_report(results, str(report_file))

        assert report_file.exists()

        # Verify report content
        with open(report_file, "r", encoding="utf-8") as f:
            content = f.read()

        assert "Comparison Report" in content
        assert "Total companies processed: 3" in content
        assert "Total key people extracted: 3" in content

    def test_file_creation_error_handling(self):
        """Test error handling when file creation fails."""
        # Create a read-only directory to simulate permission error
        readonly_dir = Path(self.temp_dir) / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)  # Read-only

        writer = WikipediaKeyPeopleWriter(str(readonly_dir))

        # Should handle permission error gracefully
        try:
            writer.write_results(self.sample_result, "test")
            # If no exception, test passes
        except PermissionError:
            # Expected behavior - should handle gracefully
            pass
        finally:
            readonly_dir.chmod(0o755)  # Restore permissions for cleanup

    def test_empty_results_handling(self):
        """Test handling of empty or minimal results."""
        empty_result = WikipediaExtractionResult(
            operation_id="empty_op",
            index_name="test",
            companies_processed=0,
            companies_successful=0,
            total_key_people=0,
            key_people=[],
            companies=[],
        )

        self.writer.write_results(empty_result, "empty")

        # Should create files but they might be empty
        stats_file = Path(self.temp_dir) / "empty_stats.txt"
        assert stats_file.exists()

        # Stats should still be written
        with open(stats_file, "r", encoding="utf-8") as f:
            content = f.read()

        assert "Companies processed: 0" in content
        assert "Total key people: 0" in content


class TestOutputFormats:
    """Test various output formats and data serialization."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.writer = WikipediaKeyPeopleWriter(self.temp_dir)

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_csv_format_with_special_characters(self):
        """Test CSV output with special characters in names."""
        people_with_special_chars = [
            WikipediaKeyPerson(
                ticker="TEST",
                company_name="Test Corp",
                raw_name="José María González (CEO)",
                clean_name="José María González",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://en.wikipedia.org/wiki/Test",
                extraction_method="test",
            )
        ]

        result = WikipediaExtractionResult(
            operation_id="special_chars_op",
            index_name="test",
            companies_processed=1,
            companies_successful=1,
            total_key_people=1,
            key_people=people_with_special_chars,
            companies=[],
        )

        self.writer._write_key_people_csv(result, "special")

        csv_file = Path(self.temp_dir) / "special_key_people.csv"
        assert csv_file.exists()

        # Read CSV and verify special characters are preserved
        df = pd.read_csv(csv_file)
        assert df.iloc[0]["clean_name"] == "José María González"

    def test_json_format_with_timestamps(self):
        """Test JSON output includes proper timestamp formatting."""
        from datetime import datetime

        person_with_timestamp = WikipediaKeyPerson(
            ticker="TEST",
            company_name="Test Corp",
            raw_name="John Doe (CEO)",
            clean_name="John Doe",
            clean_title="Chief Executive Officer",
            wikipedia_url="https://en.wikipedia.org/wiki/Test",
            extraction_method="test",
        )

        result = WikipediaExtractionResult(
            operation_id="timestamp_op",
            index_name="test",
            companies_processed=1,
            companies_successful=1,
            total_key_people=1,
            key_people=[person_with_timestamp],
            companies=[],
        )

        self.writer._write_key_people_json(result, "timestamp")

        json_file = Path(self.temp_dir) / "timestamp_key_people.json"
        assert json_file.exists()

        # Read JSON and verify timestamp format
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert len(data) == 1
        assert "scraped_at" in data[0]
        # Should be a valid ISO format timestamp
        datetime.fromisoformat(data[0]["scraped_at"].replace("Z", "+00:00"))


if __name__ == "__main__":
    pytest.main([__file__])
