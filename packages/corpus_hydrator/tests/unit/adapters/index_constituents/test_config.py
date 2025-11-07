"""
Tests for Index Constituents Configuration

Tests the configuration management and validation for index constituents.
"""

import pytest
from corpus_hydrator.adapters.index_constituents.config import (
    INDEX_CONFIGS,
    IndexConfig,
    get_available_indexes,
    get_index_config,
    validate_index_name,
)


class TestIndexConfig:
    """Test the IndexConfig dataclass."""

    def test_index_config_creation(self):
        """Test creating an IndexConfig instance."""
        config = IndexConfig(
            name="Test Index",
            url="https://example.com",
            table_id="test-table",
            table_class="test-class",
            columns=["Symbol", "Company"],
            extract_columns=["Symbol"],
        )

        assert config.name == "Test Index"
        assert config.url == "https://example.com"
        assert config.table_id == "test-table"
        assert config.table_class == "test-class"
        assert config.columns == ["Symbol", "Company"]
        assert config.extract_columns == ["Symbol"]

    def test_table_selector_property(self):
        """Test the table_selector property."""
        config = IndexConfig(
            name="Test Index",
            url="https://example.com",
            table_id="test-table",
            table_class="test-class",
            columns=["Symbol"],
            extract_columns=["Symbol"],
        )

        assert config.table_selector == "table#test-table"


class TestIndexConfigs:
    """Test the predefined index configurations."""

    def test_sp500_config(self):
        """Test S&P 500 configuration."""
        config = INDEX_CONFIGS["sp500"]
        assert config.name == "S&P 500"
        assert "wikipedia.org" in config.url
        assert config.table_id == "constituents"
        assert "Symbol" in config.columns
        assert "Security" in config.extract_columns

    def test_dow_config(self):
        """Test Dow Jones configuration."""
        config = INDEX_CONFIGS["dow"]
        assert config.name == "Dow Jones Industrial Average"
        assert "wikipedia.org" in config.url
        assert config.table_id == "constituents"
        assert "Symbol" in config.columns
        assert "Company" in config.extract_columns

    def test_nasdaq100_config(self):
        """Test Nasdaq 100 configuration."""
        config = INDEX_CONFIGS["nasdaq100"]
        assert config.name == "Nasdaq 100"
        assert "wikipedia.org" in config.url
        assert config.table_id == "constituents"
        assert "Symbol" in config.columns
        assert "Company" in config.extract_columns


class TestConfigFunctions:
    """Test configuration utility functions."""

    def test_get_index_config_valid(self):
        """Test getting config for valid index."""
        config = get_index_config("sp500")
        assert config.name == "S&P 500"

    def test_get_index_config_invalid(self):
        """Test getting config for invalid index."""
        with pytest.raises(ValueError, match="Unknown index"):
            get_index_config("invalid_index")

    def test_get_available_indexes(self):
        """Test getting list of available indexes."""
        indexes = get_available_indexes()
        assert isinstance(indexes, list)
        assert "sp500" in indexes
        assert "dow" in indexes
        assert "nasdaq100" in indexes

    def test_validate_index_name_valid(self):
        """Test validating valid index names."""
        assert validate_index_name("sp500") is True
        assert validate_index_name("dow") is True
        assert validate_index_name("nasdaq100") is True

    def test_validate_index_name_invalid(self):
        """Test validating invalid index names."""
        assert validate_index_name("invalid") is False
        assert validate_index_name("") is False
        assert validate_index_name(None) is False
