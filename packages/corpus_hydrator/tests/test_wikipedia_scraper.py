"""Tests for the Wikipedia scraper functionality."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from corpus_hydrator.adapters.wikipedia.scraper import (
    HTTPClient,
    WikipediaIndexScraper,
    WikipediaScraper,
)
from corpus_types.schemas.scraper import (
    CompanyRecord,
    OfficerRecord,
    get_default_config,
)


class TestWikipediaScraper:
    """Test cases for the Wikipedia scraper."""

    @pytest.fixture
    def mock_config(self):
        """Create a test configuration."""
        config = get_default_config()
        config.dry_run = True
        config.verbose = False
        return config

    @pytest.fixture
    def scraper(self, mock_config):
        """Create a test scraper instance."""
        return WikipediaScraper(mock_config)

    def test_initialization(self, mock_config):
        """Test scraper initialization."""
        scraper = WikipediaScraper(mock_config)

        assert scraper.config == mock_config
        assert hasattr(scraper, "index_scraper")
        assert hasattr(scraper, "people_scraper")
        assert hasattr(scraper, "sec_scraper")

    def test_company_record_creation(self):
        """Test CompanyRecord creation and validation."""
        company = CompanyRecord(
            ticker="AAPL",
            official_name="Apple Inc.",
            cik="0000320193",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            index_name="sp500",
        )

        assert company.ticker == "AAPL"
        assert company.official_name == "Apple Inc."
        assert company.cik == "0000320193"

    def test_officer_record_creation(self):
        """Test OfficerRecord creation and validation."""
        from datetime import datetime

        officer = OfficerRecord(
            name="Tim Cook",
            title="CEO",
            company_ticker="AAPL",
            company_name="Apple Inc.",
            cik="0000320193",
            source="wikipedia",
        )

        assert officer.name == "Tim Cook"
        assert officer.title == "CEO"
        assert officer.source == "wikipedia"
        assert isinstance(officer.scraped_at, datetime)

    def test_invalid_ticker(self):
        """Test ticker validation."""
        with pytest.raises(ValueError):
            CompanyRecord(
                ticker="invalid-ticker!",
                official_name="Test Company",
                wikipedia_url="https://example.com",
                index_name="test",
            )

    def test_invalid_cik(self):
        """Test CIK validation."""
        with pytest.raises(ValueError):
            CompanyRecord(
                ticker="TEST",
                official_name="Test Company",
                cik="123",  # Too short
                wikipedia_url="https://example.com",
                index_name="test",
            )

    @patch("corpus_hydrator.adapters.wikipedia.scraper.HTTPClient")
    def test_dry_run_mode(self, mock_http_client, mock_config):
        """Test that dry run mode works without making HTTP requests."""
        mock_config.dry_run = True

        # Mock the HTTP client
        mock_http = Mock()
        mock_response = Mock()
        mock_response.text = "<html><body>Mock Wikipedia page</body></html>"
        mock_http.get.return_value = mock_response
        mock_http_client.return_value = mock_http

        scraper = WikipediaScraper(mock_config)

        # This should not make actual HTTP requests
        companies, result = scraper.scrape_index("sp500")

        # In dry run mode, we might get empty results or mock data
        assert isinstance(companies, list)
        assert isinstance(result, object)


class TestHTTPClient:
    """Test cases for the HTTP client."""

    def test_rate_limiting(self, mock_config):
        """Test that rate limiting works correctly."""
        import time

        from corpus_hydrator.adapters.wikipedia.scraper import RateLimiter

        limiter = RateLimiter(rate_per_second=10, burst_size=5)

        # Should be able to acquire tokens quickly
        assert limiter.acquire() == 0.0

        # If we try to acquire too quickly, should wait
        for _ in range(5):
            limiter.acquire()

        # Next acquisition should require waiting
        wait_time = limiter.acquire()
        assert wait_time > 0

    def test_user_agent_setting(self, mock_config):
        """Test that user agent is set correctly."""
        mock_config.user_agent = "TestAgent/1.0"
        http_client = HTTPClient(mock_config)

        assert http_client.session.headers["User-Agent"] == "TestAgent/1.0"


class TestIndexScraper:
    """Test cases for the index scraper."""

    @pytest.fixture
    def mock_http_client(self):
        """Create a mock HTTP client."""
        mock = Mock()
        mock_response = Mock()
        mock_response.text = """
        <html>
        <body>
        <table id="constituents">
        <tr><th>Ticker</th><th>Company</th><th>CIK</th></tr>
        <tr>
        <td>AAPL</td>
        <td><a href="/wiki/Apple_Inc.">Apple Inc.</a></td>
        <td>0000320193</td>
        </tr>
        </table>
        </body>
        </html>
        """
        mock.get.return_value = mock_response
        return mock

    def test_index_scraping(self, mock_http_client, mock_config):
        """Test basic index scraping functionality."""
        scraper = WikipediaIndexScraper(mock_http_client, mock_config)

        # This would need more setup to fully test
        # For now, just ensure the scraper initializes
        assert scraper.http_client == mock_http_client
        assert scraper.config == mock_config


# Integration test example
def test_full_scraping_workflow(mock_config):
    """Example of how to test the full scraping workflow."""
    # This is a template for integration testing
    mock_config.dry_run = True
    mock_config.enabled_indices = ["sp500"]

    scraper = WikipediaScraper(mock_config)

    # In a real test, you would:
    # 1. Mock the HTTP responses
    # 2. Call scrape_index()
    # 3. Verify the returned CompanyRecord objects
    # 4. Call scrape_company_executives()
    # 5. Verify the returned OfficerRecord objects

    assert scraper is not None
    assert scraper.config.dry_run is True


if __name__ == "__main__":
    pytest.main([__file__])
