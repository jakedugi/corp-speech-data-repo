"""
Unit tests for Wikipedia Key People Configuration

Tests cover:
- Configuration loading and validation
- Index-specific configurations
- Scraping behavior configuration
- Content extraction configuration
"""

from unittest.mock import patch

import pytest
from corpus_hydrator.adapters.wikipedia_key_people.core.scraper import (
    EnhancedWikipediaKeyPeopleExtractor,
    WikipediaLinkExtractor,
)
from corpus_types.schemas.wikipedia_key_people import (
    WikipediaContentConfig,
    WikipediaIndexConfig,
    WikipediaKeyPeopleConfig,
    WikipediaScrapingConfig,
    get_default_config,
    get_multi_index_config,
    get_sp500_config,
    validate_config,
)


class TestConfigurationLoading:
    """Test configuration loading and initialization."""

    def test_default_config_creation(self):
        """Test creation of default configuration."""
        config = get_default_config()

        assert config.version == "1.0.0"
        assert "sp500" in config.enabled_indices
        assert config.scraping.wikipedia_rate_limit == 1.0
        assert config.scraping.max_people_per_company == 100
        assert len(config.content.role_keywords) > 10

    def test_sp500_config_creation(self):
        """Test creation of S&P 500 specific configuration."""
        config = get_sp500_config()

        assert "sp500" in config.enabled_indices
        assert len(config.enabled_indices) == 1
        assert config.indices["sp500"].name == "S&P 500"

    def test_multi_index_config_creation(self):
        """Test creation of multi-index configuration."""
        config = get_multi_index_config()

        assert "sp500" in config.enabled_indices
        assert "dow" in config.enabled_indices
        assert len(config.enabled_indices) >= 2
        assert config.scraping.max_companies == 100

    def test_custom_config_creation(self):
        """Test creation of custom configuration."""
        config = WikipediaKeyPeopleConfig(
            version="2.0.0",
            enabled_indices=["dow"],
            scraping=WikipediaScrapingConfig(
                wikipedia_rate_limit=2.0, max_companies=50, max_people_per_company=25
            ),
        )

        assert config.version == "2.0.0"
        assert config.enabled_indices == ["dow"]
        assert config.scraping.wikipedia_rate_limit == 2.0
        assert config.scraping.max_companies == 50
        assert config.scraping.max_people_per_company == 25


class TestIndexConfiguration:
    """Test index-specific configuration."""

    def test_sp500_index_config(self):
        """Test S&P 500 index configuration."""
        config = get_default_config()
        index_config = config.indices["sp500"]

        assert index_config.name == "S&P 500"
        assert index_config.short_name == "sp500"
        assert "wikipedia.org" in index_config.wikipedia_url
        assert index_config.table_id == "constituents"
        assert index_config.ticker_column == 0
        assert index_config.name_column == 1

    def test_dow_index_config(self):
        """Test Dow Jones index configuration."""
        config = get_default_config()
        index_config = config.indices["dow"]

        assert index_config.name == "Dow Jones Industrial Average"
        assert index_config.short_name == "dow"
        assert "wikipedia.org" in index_config.wikipedia_url
        assert index_config.table_id == "constituents"

    def test_nasdaq_index_config(self):
        """Test NASDAQ index configuration."""
        config = get_default_config()
        index_config = config.indices["nasdaq100"]

        assert index_config.name == "Nasdaq 100"
        assert index_config.short_name == "nasdaq100"
        assert "wikipedia.org" in index_config.wikipedia_url
        assert index_config.table_id == "constituents"

    def test_get_index_config_method(self):
        """Test get_index_config method."""
        config = get_default_config()

        # Valid index
        sp500_config = config.get_index_config("sp500")
        assert sp500_config.name == "S&P 500"

        # Invalid index
        with pytest.raises(ValueError):
            config.get_index_config("invalid_index")

    def test_get_active_indices_method(self):
        """Test get_active_indices method."""
        config = get_default_config()

        active_indices = config.get_active_indices()
        assert len(active_indices) > 0

        # Check that all active indices have configs
        for index_config in active_indices:
            assert index_config.short_name in config.enabled_indices


class TestScrapingConfiguration:
    """Test scraping behavior configuration."""

    def test_default_scraping_config(self):
        """Test default scraping configuration."""
        config = WikipediaScrapingConfig()

        assert config.wikipedia_rate_limit == 1.0
        assert config.request_timeout == 10
        assert config.max_retries == 5
        assert config.backoff_factor == 1.0
        assert config.pool_connections == 50
        assert config.pool_maxsize == 50
        assert config.max_people_per_company == 100

    def test_custom_scraping_config(self):
        """Test custom scraping configuration."""
        config = WikipediaScrapingConfig(
            wikipedia_rate_limit=2.0,
            request_timeout=15,
            max_retries=3,
            max_people_per_company=50,
        )

        assert config.wikipedia_rate_limit == 2.0
        assert config.request_timeout == 15
        assert config.max_retries == 3
        assert config.max_people_per_company == 50


class TestContentConfiguration:
    """Test content extraction configuration."""

    def test_default_content_config(self):
        """Test default content configuration."""
        config = WikipediaContentConfig()

        assert len(config.role_keywords) > 10
        assert "key people" in config.role_keywords
        assert "executive" in config.role_keywords
        assert "board" in config.role_keywords

        assert "CEO" in config.title_normalization
        assert "CFO" in config.title_normalization
        assert config.title_normalization["CEO"] == "Chief Executive Officer"

        assert len(config.name_title_patterns) > 0
        assert isinstance(config.name_title_patterns, list)

    def test_custom_content_config(self):
        """Test custom content configuration."""
        custom_keywords = ["ceo", "president", "founder"]
        custom_patterns = [r"^([^,]+),\s*(.+)$"]

        config = WikipediaContentConfig(
            role_keywords=custom_keywords, name_title_patterns=custom_patterns
        )

        assert config.role_keywords == custom_keywords
        assert config.name_title_patterns == custom_patterns


class TestConfigurationValidation:
    """Test configuration validation."""

    def test_validate_valid_config(self):
        """Test validation of valid configuration."""
        config = get_default_config()
        issues = validate_config(config)

        assert len(issues) == 0

    def test_validate_invalid_rate_limit(self):
        """Test validation of invalid rate limit."""
        config = get_default_config()
        config.scraping.wikipedia_rate_limit = 15.0  # Too high

        issues = validate_config(config)

        assert len(issues) > 0
        assert any("rate limit" in issue.lower() for issue in issues)

    def test_validate_invalid_max_people(self):
        """Test validation of invalid max people setting."""
        config = get_default_config()
        config.scraping.max_people_per_company = 0  # Invalid

        issues = validate_config(config)

        assert len(issues) > 0
        assert any("max people" in issue.lower() for issue in issues)

    def test_validate_missing_index(self):
        """Test validation with missing index configuration."""
        config = get_default_config()
        config.enabled_indices = ["invalid_index"]

        issues = validate_config(config)

        assert len(issues) > 0
        assert any("not found" in issue.lower() for issue in issues)


class TestConfigurationIntegration:
    """Test configuration integration with scrapers."""

    @patch("corpus_hydrator.adapters.wikipedia_key_people.core.scraper.session")
    def test_config_integration_with_link_extractor(self, mock_session):
        """Test that configuration integrates properly with link extractor."""
        config = get_default_config()
        config.enabled_indices = ["sp500"]

        extractor = WikipediaLinkExtractor(config)

        assert extractor.config == config
        assert extractor.config.enabled_indices == ["sp500"]

    @patch(
        "corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper.session"
    )
    def test_config_integration_with_people_extractor(self, mock_session):
        """Test that configuration integrates properly with people extractor."""
        config = get_default_config()
        config.scraping.max_people_per_company = 25

        extractor = EnhancedWikipediaKeyPeopleExtractor(config)

        assert extractor.config == config
        assert extractor.config.scraping.max_people_per_company == 25

    def test_config_modification_effects(self):
        """Test that configuration modifications affect scraper behavior."""
        config = get_default_config()

        # Modify rate limiting
        config.scraping.wikipedia_rate_limit = 2.0

        # Modify keywords
        config.content.role_keywords = ["ceo", "president"]

        # Create extractor with modified config
        with patch(
            "corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper.session"
        ):
            extractor = EnhancedWikipediaKeyPeopleExtractor(config)

        assert extractor.config.scraping.wikipedia_rate_limit == 2.0
        assert extractor.config.content.role_keywords == ["ceo", "president"]


class TestConfigurationEdgeCases:
    """Test configuration edge cases."""

    def test_empty_enabled_indices(self):
        """Test configuration with empty enabled indices."""
        config = WikipediaKeyPeopleConfig(enabled_indices=[])

        active_indices = config.get_active_indices()
        assert len(active_indices) == 0

    def test_single_index_config(self):
        """Test configuration with single index."""
        config = WikipediaKeyPeopleConfig(enabled_indices=["sp500"])

        active_indices = config.get_active_indices()
        assert len(active_indices) == 1
        assert active_indices[0].short_name == "sp500"

    def test_all_indices_config(self):
        """Test configuration with all available indices."""
        config = get_default_config()
        config.enabled_indices = list(config.indices.keys())

        active_indices = config.get_active_indices()
        assert len(active_indices) == len(config.indices)

    def test_duplicate_indices(self):
        """Test configuration with duplicate indices."""
        config = WikipediaKeyPeopleConfig(enabled_indices=["sp500", "sp500", "dow"])

        active_indices = config.get_active_indices()
        # Should still work, duplicates are handled gracefully
        assert len(active_indices) >= 1


class TestConfigurationSerialization:
    """Test configuration serialization and persistence."""

    def test_config_dict_conversion(self):
        """Test converting configuration to dictionary."""
        config = get_default_config()

        # Convert to dict (this tests that all fields are serializable)
        config_dict = config.dict()

        assert "version" in config_dict
        assert "enabled_indices" in config_dict
        assert "scraping" in config_dict
        assert "content" in config_dict
        assert isinstance(config_dict["scraping"], dict)
        assert isinstance(config_dict["content"], dict)

    def test_config_json_serialization(self):
        """Test JSON serialization of configuration."""
        config = get_default_config()

        # Convert to JSON string
        json_str = config.json()

        assert isinstance(json_str, str)
        assert '"version"' in json_str
        assert '"enabled_indices"' in json_str

        # Test round-trip
        import json

        parsed = json.loads(json_str)
        assert parsed["version"] == "1.0.0"


if __name__ == "__main__":
    pytest.main([__file__])
