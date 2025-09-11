"""
Tests for Index Constituents Data Models

Tests the data models and validation for index constituents.
"""

import pytest
from datetime import datetime
from corpus_types.schemas.models import (
    IndexConstituent,
    IndexExtractionResult,
    IndexConstituentFilter
)


class TestIndexConstituent:
    """Test the IndexConstituent data model."""

    def test_valid_constituent_creation(self):
        """Test creating a valid IndexConstituent."""
        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            sector="Technology",
            industry="Consumer Electronics",
            date_added="2023-12-15",
            source_url="https://example.com"
        )

        assert constituent.symbol == "AAPL"
        assert constituent.company_name == "Apple Inc."
        assert constituent.index_name == "S&P 500"
        assert constituent.sector == "Technology"
        assert constituent.industry == "Consumer Electronics"
        assert constituent.date_added == "2023-12-15"
        assert constituent.source_url == "https://example.com"
        assert isinstance(constituent.extracted_at, datetime)

    def test_symbol_validation_uppercase(self):
        """Test symbol validation converts to uppercase."""
        constituent = IndexConstituent(
            symbol="aapl",
            company_name="Apple Inc.",
            index_name="S&P 500",
            source_url="https://example.com"
        )

        assert constituent.symbol == "AAPL"

    def test_symbol_validation_strips_whitespace(self):
        """Test symbol validation strips whitespace."""
        constituent = IndexConstituent(
            symbol="  AAPL  ",
            company_name="Apple Inc.",
            index_name="S&P 500",
            source_url="https://example.com"
        )

        assert constituent.symbol == "AAPL"

    def test_invalid_symbol_empty(self):
        """Test validation fails for empty symbol."""
        with pytest.raises(ValueError, match="Symbol cannot be empty"):
            IndexConstituent(
                symbol="",
                company_name="Apple Inc.",
                index_name="S&P 500",
                source_url="https://example.com"
            )

    def test_invalid_symbol_whitespace_only(self):
        """Test validation fails for whitespace-only symbol."""
        with pytest.raises(ValueError, match="Symbol cannot be empty"):
            IndexConstituent(
                symbol="   ",
                company_name="Apple Inc.",
                index_name="S&P 500",
                source_url="https://example.com"
            )

    def test_invalid_company_name_empty(self):
        """Test validation fails for empty company name."""
        with pytest.raises(ValueError, match="Company name cannot be empty"):
            IndexConstituent(
                symbol="AAPL",
                company_name="",
                index_name="S&P 500",
                source_url="https://example.com"
            )

    def test_date_validation_iso_format(self):
        """Test date validation with ISO format."""
        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            date_added="2023-12-15",
            source_url="https://example.com"
        )

        assert constituent.date_added == "2023-12-15"

    def test_date_validation_us_format(self):
        """Test date validation with US format."""
        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            date_added="12/15/2023",
            source_url="https://example.com"
        )

        assert constituent.date_added == "12/15/2023"

    def test_date_validation_full_format(self):
        """Test date validation with full month format."""
        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            date_added="December 15, 2023",
            source_url="https://example.com"
        )

        assert constituent.date_added == "December 15, 2023"

    def test_date_validation_invalid_format(self):
        """Test date validation with invalid format (should still accept)."""
        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            date_added="invalid-date",
            source_url="https://example.com"
        )

        assert constituent.date_added == "invalid-date"

    def test_to_dict(self):
        """Test converting to dictionary."""
        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            source_url="https://example.com"
        )

        data = constituent.dict()
        assert data["symbol"] == "AAPL"
        assert data["company_name"] == "Apple Inc."
        assert data["index_name"] == "S&P 500"
        assert "extracted_at" in data

    def test_json_serialization(self):
        """Test JSON serialization."""
        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            source_url="https://example.com"
        )

        json_str = constituent.json()
        assert "AAPL" in json_str
        assert "Apple Inc." in json_str


class TestIndexExtractionResult:
    """Test the IndexExtractionResult data model."""

    def test_successful_result(self):
        """Test creating a successful extraction result."""
        result = IndexExtractionResult(
            index_name="S&P 500",
            total_constituents=503,
            constituents=[{"symbol": "AAPL", "company_name": "Apple Inc."}]
        )

        assert result.index_name == "S&P 500"
        assert result.total_constituents == 503
        assert result.success is True
        assert result.error_message is None
        assert len(result.constituents) == 1

    def test_failed_result(self):
        """Test creating a failed extraction result."""
        result = IndexExtractionResult(
            index_name="S&P 500",
            total_constituents=0,
            success=False,
            error_message="Table not found"
        )

        assert result.index_name == "S&P 500"
        assert result.total_constituents == 0
        assert result.success is False
        assert result.error_message == "Table not found"


class TestIndexConstituentFilter:
    """Test the IndexConstituentFilter functionality."""

    def test_empty_filter(self):
        """Test filter with no criteria."""
        filter_obj = IndexConstituentFilter()

        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            source_url="https://example.com"
        )

        assert filter_obj.matches(constituent) is True

    def test_symbol_filter_match(self):
        """Test symbol filter with matching symbol."""
        filter_obj = IndexConstituentFilter(symbols=["AAPL", "MSFT"])

        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            source_url="https://example.com"
        )

        assert filter_obj.matches(constituent) is True

    def test_symbol_filter_no_match(self):
        """Test symbol filter with non-matching symbol."""
        filter_obj = IndexConstituentFilter(symbols=["MSFT", "GOOGL"])

        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            source_url="https://example.com"
        )

        assert filter_obj.matches(constituent) is False

    def test_sector_filter_match(self):
        """Test sector filter with matching sector."""
        filter_obj = IndexConstituentFilter(sectors=["Technology"])

        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            sector="Technology",
            source_url="https://example.com"
        )

        assert filter_obj.matches(constituent) is True

    def test_sector_filter_no_match(self):
        """Test sector filter with non-matching sector."""
        filter_obj = IndexConstituentFilter(sectors=["Financials"])

        constituent = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            sector="Technology",
            source_url="https://example.com"
        )

        assert filter_obj.matches(constituent) is False

    def test_combined_filters(self):
        """Test multiple filters combined."""
        filter_obj = IndexConstituentFilter(
            symbols=["AAPL", "MSFT"],
            sectors=["Technology"]
        )

        # Should match
        constituent1 = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            sector="Technology",
            source_url="https://example.com"
        )

        # Should not match (wrong sector)
        constituent2 = IndexConstituent(
            symbol="AAPL",
            company_name="Apple Inc.",
            index_name="S&P 500",
            sector="Financials",
            source_url="https://example.com"
        )

        # Should not match (wrong symbol)
        constituent3 = IndexConstituent(
            symbol="GOOGL",
            company_name="Alphabet Inc.",
            index_name="S&P 500",
            sector="Technology",
            source_url="https://example.com"
        )

        assert filter_obj.matches(constituent1) is True
        assert filter_obj.matches(constituent2) is False
        assert filter_obj.matches(constituent3) is False
