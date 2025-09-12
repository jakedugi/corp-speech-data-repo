"""
Unit tests for Wikipedia Key People CLI Commands

Tests cover:
- CLI command execution
- Argument parsing
- Error handling
- Output formatting
"""

import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from corpus_hydrator.adapters.wikipedia_key_people.cli.commands import app
from corpus_types.schemas.wikipedia_key_people import (
    WikipediaKeyPerson,
    WikipediaCompany,
    WikipediaExtractionResult
)


class TestScrapeIndexCommand:
    """Test the scrape-index CLI command."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch('corpus_hydrator.adapters.wikipedia_key_people.cli.commands.WikipediaKeyPeopleScraper')
    def test_scrape_index_success(self, mock_scraper_class):
        """Test successful index scraping."""
        # Mock scraper and result
        mock_scraper = MagicMock()
        mock_scraper_class.return_value = mock_scraper

        # Create mock people and companies
        mock_people = [
            WikipediaKeyPerson(
                ticker="AAPL",
                company_name="Apple Inc.",
                raw_name="Tim Cook (CEO)",
                clean_name="Tim Cook",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
                extraction_method="test"
            )
        ]

        mock_companies = [
            WikipediaCompany(
                ticker="AAPL",
                company_name="Apple Inc.",
                wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
                index_name="sp500",
                key_people_count=1,
                processing_success=True
            )
        ]

        mock_result = WikipediaExtractionResult(
            operation_id="test_op",
            index_name="sp500",
            companies_processed=1,
            companies_successful=1,
            total_key_people=1,
            key_people=mock_people,
            companies=mock_companies
        )

        mock_scraper.scrape_index.return_value = mock_result

        # Run command
        with patch('pathlib.Path.mkdir'):
            with patch('pandas.DataFrame.to_csv'):
                result = self.runner.invoke(app, [
                    'scrape-index',
                    '--index', 'sp500',
                    '--output-dir', '/tmp/test'
                ])

        assert result.exit_code == 0
        assert "✅ Extraction Complete!" in result.output
        assert "Companies processed: 1" in result.output
        assert "Total key people: 1" in result.output

    @patch('corpus_hydrator.adapters.wikipedia_key_people.cli.commands.WikipediaKeyPeopleScraper')
    def test_scrape_index_with_options(self, mock_scraper_class):
        """Test index scraping with various options."""
        mock_scraper = MagicMock()
        mock_scraper_class.return_value = mock_scraper

        mock_result = WikipediaExtractionResult(
            operation_id="test_op",
            index_name="dow",
            companies_processed=2,
            companies_successful=2,
            total_key_people=3,
            key_people=[],
            companies=[]
        )

        mock_scraper.scrape_index.return_value = mock_result

        # Run command with options
        with patch('pathlib.Path.mkdir'):
            with patch('pandas.DataFrame.to_csv'):
                result = self.runner.invoke(app, [
                    'scrape-index',
                    '--index', 'dow',
                    '--max-companies', '5',
                    '--verbose',
                    '--dry-run',
                    '--output-dir', '/tmp/test'
                ])

        assert result.exit_code == 0
        mock_scraper_class.assert_called_once()
        # Check that config was created with correct options
        call_args = mock_scraper_class.call_args
        config = call_args[0][0]  # First positional argument
        assert config.dry_run == True
        assert config.verbose == True

    @patch('corpus_hydrator.adapters.wikipedia_key_people.cli.commands.WikipediaKeyPeopleScraper')
    def test_scrape_index_error_handling(self, mock_scraper_class):
        """Test error handling in index scraping."""
        mock_scraper = MagicMock()
        mock_scraper_class.return_value = mock_scraper

        # Mock failed result
        mock_result = WikipediaExtractionResult(
            operation_id="test_op",
            index_name="sp500",
            success=False,
            error_message="Test error"
        )

        mock_scraper.scrape_index.return_value = mock_result

        # Run command
        result = self.runner.invoke(app, [
            'scrape-index',
            '--index', 'sp500',
            '--output-dir', '/tmp/test'
        ])

        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "❌ Extraction failed" in result.output

    def test_scrape_index_invalid_index(self):
        """Test handling of invalid index name."""
        # This should work since the scraper handles unknown indices
        result = self.runner.invoke(app, [
            'scrape-index',
            '--index', 'invalid_index',
            '--output-dir', '/tmp/test'
        ])

        # Should not crash, but may show error from scraper
        assert result.exit_code == 0


class TestScrapeMultipleCommand:
    """Test the scrape-multiple CLI command."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch('corpus_hydrator.adapters.wikipedia_key_people.cli.commands.WikipediaKeyPeopleScraper')
    def test_scrape_multiple_success(self, mock_scraper_class):
        """Test successful multiple index scraping."""
        mock_scraper = MagicMock()
        mock_scraper_class.return_value = mock_scraper

        # Mock results for multiple indices
        mock_result1 = WikipediaExtractionResult(
            operation_id="test_sp500",
            index_name="sp500",
            companies_processed=2,
            companies_successful=2,
            total_key_people=3,
            key_people=[],
            companies=[]
        )

        mock_result2 = WikipediaExtractionResult(
            operation_id="test_dow",
            index_name="dow",
            companies_processed=1,
            companies_successful=1,
            total_key_people=2,
            key_people=[],
            companies=[]
        )

        mock_scraper.scrape_multiple_indices.return_value = {
            "sp500": mock_result1,
            "dow": mock_result2
        }

        # Run command
        with patch('pathlib.Path.mkdir'):
            with patch('pandas.DataFrame.to_csv'):
                result = self.runner.invoke(app, [
            'scrape-multiple',
                    '--indices', 'sp500', 'dow',
                    '--output-dir', '/tmp/test'
                ])

        assert result.exit_code == 0
        assert "✅ sp500: 2/2 companies" in result.output
        assert "✅ dow: 1/1 companies" in result.output
        assert "Total companies processed: 3" in result.output
        assert "Total key people extracted: 5" in result.output

    @patch('corpus_hydrator.adapters.wikipedia_key_people.cli.commands.WikipediaKeyPeopleScraper')
    def test_scrape_multiple_with_options(self, mock_scraper_class):
        """Test multiple scraping with various options."""
        mock_scraper = MagicMock()
        mock_scraper_class.return_value = mock_scraper

        mock_scraper.scrape_multiple_indices.return_value = {}

        # Run command with options
        result = self.runner.invoke(app, [
            'scrape-multiple',
            '--indices', 'sp500',
            '--max-companies', '10',
            '--verbose',
            '--dry-run',
            '--output-dir', '/tmp/test'
        ])

        assert result.exit_code == 0
        mock_scraper_class.assert_called_once()


class TestOutputFormatting:
    """Test output formatting and file operations."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch('corpus_hydrator.adapters.wikipedia_key_people.cli.commands.WikipediaKeyPeopleScraper')
    @patch('pathlib.Path.mkdir')
    @patch('pandas.DataFrame.to_csv')
    def test_file_output_creation(self, mock_to_csv, mock_mkdir, mock_scraper_class):
        """Test that output files are created correctly."""
        mock_scraper = MagicMock()
        mock_scraper_class.return_value = mock_scraper

        mock_people = [
            WikipediaKeyPerson(
                ticker="TEST",
                company_name="Test Corp",
                raw_name="John Doe (CEO)",
                clean_name="John Doe",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://en.wikipedia.org/wiki/Test",
                extraction_method="test"
            )
        ]

        mock_result = WikipediaExtractionResult(
            operation_id="test_op",
            index_name="test",
            companies_processed=1,
            companies_successful=1,
            total_key_people=1,
            key_people=mock_people,
            companies=[]
        )

        mock_scraper.scrape_index.return_value = mock_result

        # Run command
        result = self.runner.invoke(app, [
            'scrape-index',
            '--index', 'test',
            '--output-dir', '/tmp/test_output'
        ])

        assert result.exit_code == 0

        # Check that mkdir was called
        mock_mkdir.assert_called()

        # Check that DataFrame.to_csv was called (for people data)
        mock_to_csv.assert_called()


class TestErrorScenarios:
    """Test error handling scenarios."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch('corpus_hydrator.adapters.wikipedia_key_people.cli.commands.WikipediaKeyPeopleScraper')
    def test_network_error_handling(self, mock_scraper_class):
        """Test handling of network errors."""
        mock_scraper = MagicMock()
        mock_scraper_class.return_value = mock_scraper

        # Mock network error
        mock_scraper.scrape_index.side_effect = Exception("Network connection failed")

        result = self.runner.invoke(app, [
            'scrape-index',
            '--index', 'sp500',
            '--output-dir', '/tmp/test'
        ])

        assert result.exit_code == 1  # Should exit with error
        assert "❌ Error:" in result.output

    def test_missing_required_args(self):
        """Test handling of missing required arguments."""
        result = self.runner.invoke(app, [
            'scrape-index',])

        assert result.exit_code == 2  # Click error for missing required arg
        assert "Missing option" in result.output

    def test_invalid_output_dir(self):
        """Test handling of invalid output directory."""
        # This should still work as the scraper handles path creation
        with patch('pathlib.Path.mkdir', side_effect=OSError("Permission denied")):
            result = self.runner.invoke(app, [
            'scrape-index',
                '--index', 'sp500',
                '--output-dir', '/invalid/path'
            ])

        # Should handle the error gracefully
        assert result.exit_code == 1


class TestConfigurationOptions:
    """Test various configuration options."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch('corpus_hydrator.adapters.wikipedia_key_people.cli.commands.WikipediaKeyPeopleScraper')
    def test_dry_run_mode(self, mock_scraper_class):
        """Test dry run mode configuration."""
        mock_scraper = MagicMock()
        mock_scraper_class.return_value = mock_scraper

        mock_result = WikipediaExtractionResult(
            operation_id="test_op",
            index_name="sp500"
        )

        mock_scraper.scrape_index.return_value = mock_result

        result = self.runner.invoke(app, [
            'scrape-index',
            '--index', 'sp500',
            '--dry-run',
            '--output-dir', '/tmp/test'
        ])

        assert result.exit_code == 0
        assert "dry run" in result.output.lower()

        # Check that scraper was created with dry_run=True
        call_args = mock_scraper_class.call_args
        config = call_args[0][0]
        assert config.dry_run == True

    @patch('corpus_hydrator.adapters.wikipedia_key_people.cli.commands.WikipediaKeyPeopleScraper')
    def test_verbose_mode(self, mock_scraper_class):
        """Test verbose mode configuration."""
        mock_scraper = MagicMock()
        mock_scraper_class.return_value = mock_scraper

        mock_result = WikipediaExtractionResult(
            operation_id="test_op",
            index_name="sp500"
        )

        mock_scraper.scrape_index.return_value = mock_result

        result = self.runner.invoke(app, [
            'scrape-index',
            '--index', 'sp500',
            '--verbose',
            '--output-dir', '/tmp/test'
        ])

        assert result.exit_code == 0

        # Check that scraper was created with verbose=True
        call_args = mock_scraper_class.call_args
        config = call_args[0][0]
        assert config.verbose == True


class TestHelpAndUsage:
    """Test help messages and usage information."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_main_help(self):
        """Test main command help."""
        result = self.runner.invoke(app, ['--help'])

        assert result.exit_code == 0
        assert "wikipedia-key-people" in result.output
        assert "scrape-index" in result.output
        assert "scrape-multiple" in result.output

    def test_scrape_index_help(self):
        """Test scrape-index command help."""
        result = self.runner.invoke(app, [
            'scrape-index','--help'])

        assert result.exit_code == 0
        assert "--index" in result.output
        assert "--output-dir" in result.output
        assert "--max-companies" in result.output
        assert "--dry-run" in result.output
        assert "--verbose" in result.output

    def test_scrape_multiple_help(self):
        """Test scrape-multiple command help."""
        result = self.runner.invoke(app, [
            'scrape-multiple','--help'])

        assert result.exit_code == 0
        assert "--indices" in result.output
        assert "--max-companies" in result.output
        assert "--dry-run" in result.output


if __name__ == "__main__":
    pytest.main([__file__])
