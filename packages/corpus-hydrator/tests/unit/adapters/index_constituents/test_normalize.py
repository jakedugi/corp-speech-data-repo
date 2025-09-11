"""
Tests for Data Normalization

Tests the normalize module functionality for transforming raw data into IndexConstituent objects.
"""

import pytest
from unittest.mock import Mock
from corpus_hydrator.adapters.index_constituents.normalize import (
    normalize_row,
    normalize_rows
)
from corpus_types.schemas.models import IndexConstituent


class TestNormalizeRow:
    """Test individual row normalization."""

    def test_normalize_sp500_row(self):
        """Test normalizing S&P 500 row data."""
        row = {
            'Symbol': 'AAPL',
            'Security': 'Apple Inc.',
            'GICS Sector': 'Technology',
            'GICS Sub-Industry': 'Consumer Electronics',
            'Date first added': '1982-11-30'
        }

        result = normalize_row(row, 'sp500', 'S&P 500')

        assert result is not None
        assert isinstance(result, IndexConstituent)
        assert result.symbol == 'AAPL'
        assert result.company_name == 'Apple Inc.'
        assert result.index_name == 'S&P 500'
        assert result.sector == 'Technology'
        assert result.industry == 'Consumer Electronics'
        assert result.date_added == '1982-11-30'
        assert result.source_url == 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

    def test_normalize_dow_row(self):
        """Test normalizing Dow Jones row data."""
        row = {
            'Symbol': 'AAPL',
            'Company': 'Apple Inc.',
            'Industry': 'Consumer Electronics',
            'Date added': '2015-03-19'
        }

        result = normalize_row(row, 'dow', 'Dow Jones Industrial Average')

        assert result is not None
        assert isinstance(result, IndexConstituent)
        assert result.symbol == 'AAPL'
        assert result.company_name == 'Apple Inc.'
        assert result.index_name == 'Dow Jones Industrial Average'
        assert result.sector is None  # Dow doesn't have sector
        assert result.industry == 'Consumer Electronics'
        assert result.date_added == '2015-03-19'

    def test_normalize_missing_required_fields(self):
        """Test normalizing row with missing required fields."""
        # Missing symbol
        row = {
            'Security': 'Apple Inc.',
            'GICS Sector': 'Technology'
        }

        result = normalize_row(row, 'sp500', 'S&P 500')
        assert result is None

        # Missing company name
        row = {
            'Symbol': 'AAPL',
            'GICS Sector': 'Technology'
        }

        result = normalize_row(row, 'sp500', 'S&P 500')
        assert result is None

    def test_normalize_alternate_column_names(self):
        """Test normalizing with alternate column names."""
        # Test 'Company' instead of 'Security'
        row = {
            'Symbol': 'AAPL',
            'Company': 'Apple Inc.',
            'GICS Sector': 'Technology'
        }

        result = normalize_row(row, 'sp500', 'S&P 500')
        assert result is not None
        assert result.company_name == 'Apple Inc.'

        # Test 'Ticker' instead of 'Symbol'
        row = {
            'Ticker': 'AAPL',
            'Security': 'Apple Inc.',
            'GICS Sector': 'Technology'
        }

        result = normalize_row(row, 'sp500', 'S&P 500')
        assert result is not None
        assert result.symbol == 'AAPL'

    def test_normalize_symbol_case_conversion(self):
        """Test that symbols are converted to uppercase."""
        row = {
            'Symbol': 'aapl',
            'Security': 'Apple Inc.',
            'GICS Sector': 'Technology'
        }

        result = normalize_row(row, 'sp500', 'S&P 500')
        assert result is not None
        assert result.symbol == 'AAPL'


class TestNormalizeRows:
    """Test batch row normalization."""

    def test_normalize_multiple_rows(self):
        """Test normalizing multiple rows."""
        rows = [
            {
                'Symbol': 'AAPL',
                'Security': 'Apple Inc.',
                'GICS Sector': 'Technology'
            },
            {
                'Symbol': 'MSFT',
                'Security': 'Microsoft Corp.',
                'GICS Sector': 'Technology'
            },
            {
                'Symbol': 'GOOGL',
                'Security': 'Alphabet Inc.',
                'GICS Sector': 'Communication Services'
            }
        ]

        results = normalize_rows(rows, 'sp500', 'S&P 500')

        assert len(results) == 3
        assert all(isinstance(r, IndexConstituent) for r in results)

        # Check first result
        assert results[0].symbol == 'AAPL'
        assert results[0].company_name == 'Apple Inc.'
        assert results[0].index_name == 'S&P 500'

        # Results are sorted by symbol, so check in alphabetical order
        # Check second result (GOOGL comes before MSFT alphabetically)
        assert results[1].symbol == 'GOOGL'
        assert results[1].company_name == 'Alphabet Inc.'

        # Check third result (MSFT)
        assert results[2].symbol == 'MSFT'
        assert results[2].company_name == 'Microsoft Corp.'

    def test_normalize_rows_with_invalid_data(self):
        """Test normalizing rows with some invalid data."""
        rows = [
            {
                'Symbol': 'AAPL',
                'Security': 'Apple Inc.',
                'GICS Sector': 'Technology'
            },
            {
                'Security': 'Invalid Company',  # Missing symbol
                'GICS Sector': 'Technology'
            },
            {
                'Symbol': 'MSFT',
                'Security': 'Microsoft Corp.',
                'GICS Sector': 'Technology'
            }
        ]

        results = normalize_rows(rows, 'sp500', 'S&P 500')

        # Should only return valid results
        assert len(results) == 2
        assert results[0].symbol == 'AAPL'
        assert results[1].symbol == 'MSFT'

    def test_normalize_rows_deterministic_order(self):
        """Test that results are returned in deterministic order."""
        rows = [
            {'Symbol': 'ZTEST', 'Security': 'Z Test Company'},
            {'Symbol': 'ATEST', 'Security': 'A Test Company'},
            {'Symbol': 'MTEST', 'Security': 'M Test Company'}
        ]

        results1 = normalize_rows(rows, 'sp500', 'S&P 500')
        results2 = normalize_rows(rows, 'sp500', 'S&P 500')

        # Results should be in same order both times
        assert len(results1) == len(results2) == 3
        for r1, r2 in zip(results1, results2):
            assert r1.symbol == r2.symbol
            assert r1.company_name == r2.company_name

        # Should be sorted by symbol
        assert results1[0].symbol == 'ATEST'
        assert results1[1].symbol == 'MTEST'
        assert results1[2].symbol == 'ZTEST'

    def test_normalize_empty_rows(self):
        """Test normalizing empty row list."""
        results = normalize_rows([], 'sp500', 'S&P 500')
        assert len(results) == 0
