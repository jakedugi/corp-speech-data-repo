"""
Unit tests for Wikipedia Key People Schemas

Tests cover:
- Data model validation
- Configuration validation
- Schema constraints and edge cases
"""

import pytest
from pydantic import ValidationError
from datetime import datetime

from corpus_types.schemas.wikipedia_key_people import (
    WikipediaKeyPerson,
    WikipediaCompany,
    WikipediaKeyPeopleConfig,
    WikipediaExtractionResult,
    validate_config,
    validate_key_person,
    get_default_config
)


class TestWikipediaKeyPerson:
    """Test WikipediaKeyPerson data model."""

    def test_valid_person_creation(self):
        """Test creation of valid person object."""
        person = WikipediaKeyPerson(
            ticker="AAPL",
            company_name="Apple Inc.",
            raw_name="Tim Cook (CEO)",
            clean_name="Tim Cook",
            clean_title="Chief Executive Officer",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            extraction_method="test"
        )

        assert person.ticker == "AAPL"
        assert person.clean_name == "Tim Cook"
        assert person.clean_title == "Chief Executive Officer"
        assert person.confidence_score == 1.0  # Default
        assert isinstance(person.scraped_at, datetime)

    def test_ticker_validation(self):
        """Test ticker validation."""
        # Valid tickers
        valid_tickers = ["AAPL", "MSFT", "TSLA.O", "BRK.A"]

        for ticker in valid_tickers:
            person = WikipediaKeyPerson(
                ticker=ticker,
                company_name="Test Corp",
                raw_name="John Doe",
                clean_name="John Doe",
                clean_title="CEO",
            wikipedia_url="https://example.com",
            extraction_method="test"
        )
            assert person.ticker == ticker.upper()

        # Invalid tickers
        invalid_tickers = ["aapl", "123", "TOOLONGTICKER", "A@P"]

        for ticker in invalid_tickers:
            with pytest.raises(ValidationError):
                WikipediaKeyPerson(
                    ticker=ticker,
                    company_name="Test Corp",
                    raw_name="John Doe",
                    clean_name="John Doe",
                    clean_title="CEO",
            wikipedia_url="https://example.com",
            extraction_method="test"
        )

    def test_name_validation(self):
        """Test name validation."""
        # Valid names
        valid_names = ["John Doe", "Mary Smith", "Jean-Pierre Dubois"]

        for name in valid_names:
            person = WikipediaKeyPerson(
                ticker="TEST",
                company_name="Test Corp",
                raw_name=name,
                clean_name=name,
                clean_title="CEO",
            wikipedia_url="https://example.com",
            extraction_method="test"
        )
            assert person.clean_name == name

        # Invalid names
        invalid_names = ["", "A", "123", "X" * 101]  # Too long

        for name in invalid_names:
            with pytest.raises(ValidationError):
                WikipediaKeyPerson(
                    ticker="TEST",
                    company_name="Test Corp",
                    raw_name="John Doe",
                    clean_name=name,
                    clean_title="CEO",
            wikipedia_url="https://example.com",
            extraction_method="test"
        )

    def test_title_validation(self):
        """Test title validation."""
        # Valid titles
        valid_titles = ["CEO", "Chief Executive Officer", "President", "Director"]

        for title in valid_titles:
            person = WikipediaKeyPerson(
                ticker="TEST",
                company_name="Test Corp",
                raw_name="John Doe",
                clean_name="John Doe",
                clean_title=title,
            wikipedia_url="https://example.com",
            extraction_method="test"
        )
            assert person.clean_title == title

        # Invalid titles
        invalid_titles = ["", "X" * 101]  # Empty or too long

        for title in invalid_titles:
            with pytest.raises(ValidationError):
                WikipediaKeyPerson(
                    ticker="TEST",
                    company_name="Test Corp",
                    raw_name="John Doe",
                    clean_name="John Doe",
                    clean_title=title,
            wikipedia_url="https://example.com",
            extraction_method="test"
        )

    def test_wikipedia_url_validation(self):
        """Test Wikipedia URL validation."""
        # Valid URLs
        valid_urls = [
            "https://en.wikipedia.org/wiki/Apple_Inc.",
            "https://en.wikipedia.org/wiki/Microsoft"
        ]

        for url in valid_urls:
            person = WikipediaKeyPerson(
                ticker="TEST",
                company_name="Test Corp",
                raw_name="John Doe",
                clean_name="John Doe",
                clean_title="CEO",
                wikipedia_url=url
            )
            assert person.wikipedia_url == url

        # Invalid URLs
        invalid_urls = [
            "https://example.com",
            "http://en.wikipedia.org/wiki/Test",
            "not-a-url"
        ]

        for url in invalid_urls:
            with pytest.raises(ValidationError):
                WikipediaKeyPerson(
                    ticker="TEST",
                    company_name="Test Corp",
                    raw_name="John Doe",
                    clean_name="John Doe",
                    clean_title="CEO",
                    wikipedia_url=url
                )

    def test_confidence_score_validation(self):
        """Test confidence score validation."""
        # Valid scores
        valid_scores = [0.0, 0.5, 1.0, 0.75]

        for score in valid_scores:
            person = WikipediaKeyPerson(
                ticker="TEST",
                company_name="Test Corp",
                raw_name="John Doe",
                clean_name="John Doe",
                clean_title="CEO",
                wikipedia_url="https://example.com",
                confidence_score=score
            )
            assert person.confidence_score == score

        # Invalid scores
        invalid_scores = [-0.1, 1.1, 2.0]

        for score in invalid_scores:
            with pytest.raises(ValidationError):
                WikipediaKeyPerson(
                    ticker="TEST",
                    company_name="Test Corp",
                    raw_name="John Doe",
                    clean_name="John Doe",
                    clean_title="CEO",
                    wikipedia_url="https://example.com",
                    confidence_score=score
                )


class TestWikipediaCompany:
    """Test WikipediaCompany data model."""

    def test_valid_company_creation(self):
        """Test creation of valid company object."""
        company = WikipediaCompany(
            ticker="AAPL",
            company_name="Apple Inc.",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            index_name="sp500"
        )

        assert company.ticker == "AAPL"
        assert company.company_name == "Apple Inc."
        assert company.index_name == "sp500"
        assert company.key_people_count == 0  # Default
        assert company.processing_success == True  # Default

    def test_company_with_people_count(self):
        """Test company with key people count."""
        company = WikipediaCompany(
            ticker="AAPL",
            company_name="Apple Inc.",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            index_name="sp500",
            key_people_count=5,
            processing_success=True
        )

        assert company.key_people_count == 5
        assert company.processing_success == True


class TestWikipediaExtractionResult:
    """Test WikipediaExtractionResult data model."""

    def test_valid_result_creation(self):
        """Test creation of valid extraction result."""
        from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPerson

        people = [
            WikipediaKeyPerson(
                ticker="AAPL",
                company_name="Apple Inc.",
                raw_name="Tim Cook (CEO)",
                clean_name="Tim Cook",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc."
            )
        ]

        result = WikipediaExtractionResult(
            operation_id="test_20250101_120000",
            index_name="sp500",
            companies_processed=1,
            companies_successful=1,
            total_key_people=1,
            key_people=people
        )

        assert result.operation_id == "test_20250101_120000"
        assert result.index_name == "sp500"
        assert result.companies_processed == 1
        assert result.companies_successful == 1
        assert result.total_key_people == 1
        assert len(result.key_people) == 1
        assert result.success == True  # Default

    def test_result_mark_completed(self):
        """Test marking result as completed."""
        result = WikipediaExtractionResult(
            operation_id="test_op",
            index_name="sp500"
        )

        # Initially not completed
        assert result.completed_at is None

        # Mark as completed
        result.mark_completed()

        # Now should have completion time
        assert result.completed_at is not None
        assert isinstance(result.completed_at, datetime)


class TestWikipediaKeyPeopleConfig:
    """Test WikipediaKeyPeopleConfig."""

    def test_default_config_creation(self):
        """Test creation of default configuration."""
        config = get_default_config()

        assert config.version == "1.0.0"
        assert "sp500" in config.enabled_indices
        assert config.scraping.wikipedia_rate_limit == 1.0
        assert config.scraping.max_people_per_company == 100

    def test_config_index_operations(self):
        """Test index configuration operations."""
        config = get_default_config()

        # Test getting index config
        sp500_config = config.get_index_config("sp500")
        assert sp500_config.name == "S&P 500"
        assert sp500_config.short_name == "sp500"

        # Test getting active indices
        active_indices = config.get_active_indices()
        assert len(active_indices) > 0
        assert active_indices[0].short_name in config.enabled_indices

        # Test invalid index
        with pytest.raises(ValueError):
            config.get_index_config("invalid_index")


class TestValidationFunctions:
    """Test validation functions."""

    def test_validate_config_valid(self):
        """Test validation of valid configuration."""
        config = get_default_config()
        issues = validate_config(config)

        assert len(issues) == 0

    def test_validate_config_invalid(self):
        """Test validation of invalid configuration."""
        config = get_default_config()
        config.scraping.wikipedia_rate_limit = -1  # Invalid

        issues = validate_config(config)

        assert len(issues) > 0
        assert any("rate limit" in issue.lower() for issue in issues)

    def test_validate_key_person_valid(self):
        """Test validation of valid key person."""
        person = WikipediaKeyPerson(
            ticker="AAPL",
            company_name="Apple Inc.",
            raw_name="Tim Cook (CEO)",
            clean_name="Tim Cook",
            clean_title="Chief Executive Officer",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            extraction_method="test"
        )

        issues = validate_key_person(person)

        assert len(issues) == 0

    def test_validate_key_person_invalid(self):
        """Test validation of invalid key person."""
        # Person with empty name
        person = WikipediaKeyPerson(
            ticker="AAPL",
            company_name="Apple Inc.",
            raw_name="",
            clean_name="",
            clean_title="CEO",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            extraction_method="test"
        )

        issues = validate_key_person(person)

        assert len(issues) > 0
        assert any("name" in issue.lower() for issue in issues)


class TestConfigurationScenarios:
    """Test various configuration scenarios."""

    def test_sp500_config(self):
        """Test S&P 500 specific configuration."""
        from corpus_types.schemas.wikipedia_key_people import get_sp500_config

        config = get_sp500_config()

        assert "sp500" in config.enabled_indices
        assert len(config.enabled_indices) == 1

    def test_multi_index_config(self):
        """Test multi-index configuration."""
        from corpus_types.schemas.wikipedia_key_people import get_multi_index_config

        config = get_multi_index_config()

        assert "sp500" in config.enabled_indices
        assert "dow" in config.enabled_indices
        assert len(config.enabled_indices) >= 2

    def test_custom_config(self):
        """Test custom configuration creation."""
        config = WikipediaKeyPeopleConfig(
            version="2.0.0",
            enabled_indices=["dow"],
            scraping=WikipediaKeyPeopleConfig.Scraping(
                wikipedia_rate_limit=2.0,
                max_companies=50
            )
        )

        assert config.version == "2.0.0"
        assert config.enabled_indices == ["dow"]
        assert config.scraping.wikipedia_rate_limit == 2.0
        assert config.scraping.max_companies == 50


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_config(self):
        """Test handling of empty configuration."""
        # This should use defaults
        config = WikipediaKeyPeopleConfig()

        assert config.version == "1.0.0"
        assert len(config.enabled_indices) > 0

    def test_extreme_values(self):
        """Test extreme configuration values."""
        # Very high rate limit (should be caught by validation)
        config = get_default_config()
        config.scraping.wikipedia_rate_limit = 100

        issues = validate_config(config)
        assert len(issues) > 0

    def test_unicode_names(self):
        """Test handling of Unicode names."""
        unicode_name = "José María"

        person = WikipediaKeyPerson(
            ticker="TEST",
            company_name="Test Corp",
            raw_name=unicode_name,
            clean_name=unicode_name,
            clean_title="CEO",
            wikipedia_url="https://en.wikipedia.org/wiki/Test"
        )

        assert person.clean_name == unicode_name


if __name__ == "__main__":
    pytest.main([__file__])
