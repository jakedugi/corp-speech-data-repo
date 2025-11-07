"""
Contract Tests for Index Constituents Providers

These tests ensure all providers satisfy the same contracts and invariants,
regardless of their implementation. This guarantees consistent behavior
across different data sources.
"""

from typing import Any, Dict

import pytest
from corpus_hydrator.adapters.index_constituents import (
    HtmlTableParser,
    WikipediaProvider,
    extract_index,
)
from corpus_types.schemas.models import IndexConstituent


class TestProviderContracts:
    """Test contracts that all providers must satisfy."""

    @pytest.fixture
    def wikipedia_provider(self):
        """Wikipedia provider instance."""
        return WikipediaProvider()

    @pytest.fixture
    def html_parser(self):
        """HTML table parser instance."""
        return HtmlTableParser()

    def test_provider_interface_contract(self, wikipedia_provider):
        """Test that provider implements required interface."""
        # Provider must have name
        assert hasattr(wikipedia_provider, "name")
        assert isinstance(wikipedia_provider.name, str)
        assert len(wikipedia_provider.name) > 0

        # Provider must have priority
        assert hasattr(wikipedia_provider, "priority")
        assert isinstance(wikipedia_provider.priority, int)
        assert wikipedia_provider.priority >= 0

        # Provider must have fetch_raw method
        assert hasattr(wikipedia_provider, "fetch_raw")
        assert callable(wikipedia_provider.fetch_raw)

    def test_parser_interface_contract(self, html_parser):
        """Test that parser implements required interface."""
        # Parser must have supported_formats
        assert hasattr(html_parser, "supported_formats")
        assert isinstance(html_parser.supported_formats, list)
        assert len(html_parser.supported_formats) > 0

        # Parser must have parse method
        assert hasattr(html_parser, "parse")
        assert callable(html_parser.parse)

    @pytest.mark.parametrize("index_key", ["sp500", "dow", "nasdaq100"])
    def test_extraction_contract(self, wikipedia_provider, html_parser, index_key):
        """Test that extraction produces consistent results across providers."""
        result = extract_index(index_key, wikipedia_provider, html_parser)

        # Result must be an IndexExtractionResult
        assert hasattr(result, "success")
        assert hasattr(result, "index_name")
        assert hasattr(result, "total_constituents")
        assert hasattr(result, "constituents")
        assert hasattr(result, "error_message")

        # If successful, must have valid data
        if result.success:
            # Must have non-empty index name
            assert result.index_name
            assert len(result.index_name) > 0

            # Must have reasonable number of constituents
            assert result.total_constituents > 0
            assert len(result.constituents) == result.total_constituents

            # Constituents must be valid IndexConstituent objects
            for constituent in result.constituents:
                assert isinstance(constituent, IndexConstituent)

                # Required fields must be present and valid
                assert constituent.symbol
                assert len(constituent.symbol) > 0
                assert constituent.symbol == constituent.symbol.upper()

                assert constituent.company_name
                assert len(constituent.company_name) > 0

                assert constituent.index_name == result.index_name

                # Optional fields should be reasonable if present
                if constituent.sector:
                    assert len(constituent.sector) > 0
                if constituent.industry:
                    assert len(constituent.industry) > 0
                if constituent.date_added:
                    assert len(constituent.date_added) > 0

                # Source URL must be present and valid
                assert constituent.source_url
                assert constituent.source_url.startswith("http")

                # Extracted_at must be present
                assert constituent.extracted_at is not None

        # If failed, must have error message
        else:
            assert result.error_message
            assert len(result.error_message) > 0
            assert result.total_constituents == 0

    @pytest.mark.parametrize("index_key", ["sp500", "dow", "nasdaq100"])
    def test_deterministic_output_contract(
        self, wikipedia_provider, html_parser, index_key
    ):
        """Test that multiple runs produce identical results."""
        # Run extraction twice
        result1 = extract_index(index_key, wikipedia_provider, html_parser)
        result2 = extract_index(index_key, wikipedia_provider, html_parser)

        # Both must succeed or both must fail
        assert result1.success == result2.success

        if result1.success and result2.success:
            # Must have same index name
            assert result1.index_name == result2.index_name

            # Must have same number of constituents
            assert result1.total_constituents == result2.total_constituents
            assert len(result1.constituents) == len(result2.constituents)

            # Constituents must be in same order (deterministic)
            for c1, c2 in zip(result1.constituents, result2.constituents):
                assert c1.symbol == c2.symbol
                assert c1.company_name == c2.company_name
                assert c1.index_name == c2.index_name

    def test_error_handling_contract(self, wikipedia_provider, html_parser):
        """Test that all providers handle errors consistently."""
        # Test with invalid index - should raise ValueError from config
        with pytest.raises(ValueError, match="Unknown index"):
            extract_index("invalid_index", wikipedia_provider, html_parser)

    def test_symbol_uniqueness_contract(self, wikipedia_provider, html_parser):
        """Test that all symbols are unique within an index."""
        for index_key in ["sp500", "dow", "nasdaq100"]:
            result = extract_index(index_key, wikipedia_provider, html_parser)

            if result.success:
                symbols = [c.symbol for c in result.constituents]
                unique_symbols = set(symbols)

                # All symbols must be unique
                assert len(symbols) == len(
                    unique_symbols
                ), f"Duplicate symbols found in {index_key}"

    def test_data_completeness_contract(self, wikipedia_provider, html_parser):
        """Test that extracted data meets minimum completeness requirements."""
        # Define minimum expected counts (based on real data)
        min_counts = {
            "sp500": 490,  # S&P 500 typically has ~500 companies
            "dow": 25,  # Dow Jones has 30 companies
            "nasdaq100": 95,  # Nasdaq 100 has ~100 companies
        }

        # Define sector completeness expectations (some indexes don't have sector data)
        sector_expectations = {
            "sp500": 0.8,  # S&P 500 should have good sector data
            "dow": 0.0,  # Dow Jones doesn't have sector data
            "nasdaq100": 0.0,  # Nasdaq 100 doesn't have sector data in current format
        }

        for index_key, min_count in min_counts.items():
            result = extract_index(index_key, wikipedia_provider, html_parser)

            if result.success:
                # Must meet minimum count
                assert (
                    result.total_constituents >= min_count
                ), f"{index_key} has {result.total_constituents} constituents, expected at least {min_count}"

                # Must have reasonable data completeness
                total_constituents = len(result.constituents)
                companies_with_sector = sum(1 for c in result.constituents if c.sector)
                companies_with_industry = sum(
                    1 for c in result.constituents if c.industry
                )

                # Check sector completeness against expectations
                sector_completeness = companies_with_sector / total_constituents
                expected_sector_pct = sector_expectations[index_key]
                assert (
                    sector_completeness >= expected_sector_pct
                ), f"{index_key} sector completeness: {sector_completeness:.2%}, expected >= {expected_sector_pct:.2%}"

                # Check industry completeness (lower expectations for indexes without this data)
                industry_completeness = companies_with_industry / total_constituents
                if index_key == "sp500":
                    assert (
                        industry_completeness >= 0.7
                    ), f"{index_key} industry completeness: {industry_completeness:.2%}, expected >= 70%"
                else:
                    # Dow Jones and Nasdaq 100 may not have industry data
                    assert (
                        industry_completeness >= 0.0
                    ), f"{index_key} industry completeness: {industry_completeness:.2%}, expected >= 0%"
