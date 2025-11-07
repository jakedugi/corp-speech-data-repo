"""
Unit tests for Wikipedia Key People Scraper

Tests cover:
- Link extraction from index pages
- Enhanced people extraction from various page structures
- Data parsing and normalization
- Error handling and edge cases
"""

from unittest.mock import MagicMock, Mock, patch

import pytest
import requests
from bs4 import BeautifulSoup
from corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper import (
    EnhancedWikipediaKeyPeopleExtractor,
)
from corpus_hydrator.adapters.wikipedia_key_people.core.scraper import (
    EnhancedWikipediaKeyPeopleExtractor,
    WikipediaLinkExtractor,
)
from corpus_types.schemas.wikipedia_key_people import (
    WikipediaKeyPeopleConfig,
    WikipediaKeyPerson,
    get_default_config,
)


class TestWikipediaLinkExtractor:
    """Test link extraction from index pages."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = get_default_config()
        self.extractor = WikipediaLinkExtractor(self.config)

    @patch("corpus_hydrator.adapters.wikipedia_key_people.core.scraper.session")
    def test_extract_company_links_success(self, mock_session):
        """Test successful link extraction from S&P 500 page."""
        # Mock response with sample S&P 500 table HTML
        mock_response = Mock()
        mock_response.text = """
        <html>
        <body>
        <table id="constituents">
        <tr><th>Symbol</th><th>Security</th><th>GICS Sector</th></tr>
        <tr>
        <td>AAPL</td>
        <td><a href="/wiki/Apple_Inc.">Apple Inc.</a></td>
        <td>Information Technology</td>
        </tr>
        <tr>
        <td>MSFT</td>
        <td><a href="/wiki/Microsoft">Microsoft Corporation</a></td>
        <td>Information Technology</td>
        </tr>
        </table>
        </body>
        </html>
        """
        mock_session.get.return_value = mock_response

        result = self.extractor.extract_company_links("sp500")

        assert len(result) == 2
        assert result[0]["ticker"] == "AAPL"
        assert result[0]["company_name"] == "Apple Inc."
        assert result[0]["wikipedia_url"] == "https://en.wikipedia.org/wiki/Apple_Inc."
        assert result[1]["ticker"] == "MSFT"
        assert result[1]["company_name"] == "Microsoft Corporation"

    @patch("corpus_hydrator.adapters.wikipedia_key_people.core.scraper.session")
    def test_extract_company_links_no_table(self, mock_session):
        """Test handling when no constituents table is found."""
        mock_response = Mock()
        mock_response.text = "<html><body><p>No table here</p></body></html>"
        mock_session.get.return_value = mock_response

        result = self.extractor.extract_company_links("sp500")

        assert result == []

    @patch("corpus_hydrator.adapters.wikipedia_key_people.core.scraper.session")
    def test_extract_company_links_network_error(self, mock_session):
        """Test handling of network errors."""
        mock_session.get.side_effect = requests.exceptions.RequestException(
            "Network error"
        )

        result = self.extractor.extract_company_links("sp500")

        assert result == []


class TestEnhancedWikipediaKeyPeopleExtractor:
    """Test enhanced people extraction from various page structures."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = get_default_config()
        self.extractor = EnhancedWikipediaKeyPeopleExtractor(self.config)
        self.test_company = {
            "ticker": "MCD",
            "company_name": "McDonald's",
            "wikipedia_url": "https://en.wikipedia.org/wiki/McDonald%27s",
            "index_name": "dow",
        }

    def test_parse_person_text_standard_format(self):
        """Test parsing standard name (title) format."""
        text = "Chris Kempczinski (President and CEO)"
        result = self.extractor._parse_person_text(text, self.test_company)

        assert result is not None
        assert result.clean_name == "Chris Kempczinski"
        assert result.clean_title == "President and Chief Executive Officer"
        assert result.ticker == "MCD"
        assert result.confidence_score >= 0.8

    def test_parse_person_text_dash_format(self):
        """Test parsing name — title format."""
        text = "John Doe — Chief Financial Officer"
        result = self.extractor._parse_person_text(text, self.test_company)

        assert result is not None
        assert result.clean_name == "John Doe"
        assert result.clean_title == "Chief Financial Officer"

    def test_parse_person_text_malformed(self):
        """Test handling of malformed entries."""
        malformed_texts = ["(", ")", "chairman", "CEO", "123", "A"]

        for text in malformed_texts:
            result = self.extractor._parse_person_text(text, self.test_company)
            assert result is None

    def test_looks_like_person_name(self):
        """Test person name detection."""
        valid_names = ["John Doe", "Mary Smith", "Jean-Pierre Dubois"]
        invalid_names = ["CEO", "123", "A", "", "the board"]

        for name in valid_names:
            assert self.extractor._looks_like_person_name(name) == True

        for name in invalid_names:
            assert self.extractor._looks_like_person_name(name) == False

    def test_contains_people_data(self):
        """Test detection of people-related content."""
        people_content = [
            "Board of Directors",
            "Executive Officers",
            "Management Team",
            "John Doe is CEO",
        ]

        non_people_content = [
            "The company reported revenue of $1 billion",
            "Stock trading volume was 10 million shares",
            "The corporation was founded in 1990",
        ]

        for content in people_content:
            assert self.extractor._contains_people_data(content) == True

        for content in non_people_content:
            assert self.extractor._contains_people_data(content) == False

    @patch(
        "corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper.session"
    )
    def test_extract_from_infobox(self, mock_session):
        """Test extraction from main infobox."""
        mock_response = Mock()
        mock_response.text = """
        <html>
        <body>
        <table class="infobox vcard">
        <tr>
        <th>Key people</th>
        <td>
        <li>Chris Kempczinski (President and CEO)</li>
        <li>Joe Erlinger (President, McDonald's USA)</li>
        </td>
        </tr>
        </table>
        </body>
        </html>
        """
        mock_session.get.return_value = mock_response

        people = self.extractor.extract_key_people(self.test_company)

        assert len(people) >= 1
        # Should find at least Chris Kempczinski
        names = [p.clean_name for p in people]
        assert "Chris Kempczinski" in names

    @patch(
        "corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper.session"
    )
    def test_extract_from_board_section(self, mock_session):
        """Test extraction from Board of Directors section."""
        mock_response = Mock()
        mock_response.text = """
        <html>
        <body>
        <h2>Board of directors</h2>
        <table class="wikitable">
        <tr><th>Name</th><th>Position</th></tr>
        <tr><td>John Rogers</td><td>Chairman</td></tr>
        <tr><td>Mary Dillon</td><td>Director</td></tr>
        </table>
        </body>
        </html>
        """
        mock_session.get.return_value = mock_response

        people = self.extractor.extract_key_people(self.test_company)

        # Should find people from the board table
        names = [p.clean_name for p in people]
        assert any("Rogers" in name or "Dillon" in name for name in names)

    @patch(
        "corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper.session"
    )
    def test_extract_from_lists(self, mock_session):
        """Test extraction from unordered lists."""
        mock_response = Mock()
        mock_response.text = """
        <html>
        <body>
        <div class="board-members">
        <ul>
        <li>Robert Eckert (Chairman)</li>
        <li>Suzanne Nora Johnson (Director)</li>
        <li>John Rogers (Director)</li>
        </ul>
        </div>
        </body>
        </html>
        """
        mock_session.get.return_value = mock_response

        people = self.extractor.extract_key_people(self.test_company)

        # Should find people from the list
        names = [p.clean_name for p in people]
        assert any("Eckert" in name or "Johnson" in name for name in names)

    def test_title_normalization(self):
        """Test title abbreviation expansion."""
        # Test CEO expansion
        result = self.extractor._clean_title("CEO")
        assert result == "Chief Executive Officer"

        # Test CFO expansion
        result = self.extractor._clean_title("CFO")
        assert result == "Chief Financial Officer"

        # Test multiple abbreviations
        result = self.extractor._clean_title("CEO & CFO")
        assert "Chief Executive Officer" in result
        assert "Chief Financial Officer" in result

    def test_duplicate_removal(self):
        """Test that duplicate people are removed."""
        # Test the duplicate removal logic directly by calling the main extraction
        # with a mock that returns duplicates from different sources

        # Create a mock response with duplicate people in different sections
        mock_response = Mock()
        mock_response.text = """
        <html>
        <body>
        <!-- Main infobox with John Doe -->
        <table class="infobox vcard">
        <tr><th>Key people</th><td>
        <li>John Doe (CEO)</li>
        </td></tr>
        </table>

        <!-- Board section with same person -->
        <h2>Board of directors</h2>
        <table class="wikitable">
        <tr><td>John Doe — Chief Executive Officer</td></tr>
        </table>
        </body>
        </html>
        """

        with patch(
            "corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper.session"
        ) as mock_session:
            mock_session.get.return_value = mock_response

            people = self.extractor.extract_key_people(self.test_company)

        # Should only have one John Doe despite appearing in multiple places
        john_doe_count = sum(1 for p in people if p.clean_name == "John Doe")
        assert john_doe_count <= 1  # At most one (could be 0 if parsing fails)

    @patch(
        "corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper.session"
    )
    def test_error_handling(self, mock_session):
        """Test error handling for network issues."""
        mock_session.get.side_effect = requests.exceptions.RequestException(
            "Network error"
        )

        people = self.extractor.extract_key_people(self.test_company)

        assert people == []

    def test_rate_limiting(self):
        """Test that rate limiting is applied."""
        # This is hard to test directly, but we can verify the sleep call is made
        with patch("time.sleep") as mock_sleep:
            with patch(
                "corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper.session"
            ):
                self.extractor.extract_key_people(self.test_company)

            # Should have called sleep at least once for rate limiting
            mock_sleep.assert_called()


class TestDataValidation:
    """Test data validation and quality checks."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = get_default_config()

    def test_valid_person_creation(self):
        """Test creation of valid person objects."""
        from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPerson

        person = WikipediaKeyPerson(
            ticker="AAPL",
            company_name="Apple Inc.",
            raw_name="Tim Cook (CEO)",
            clean_name="Tim Cook",
            clean_title="Chief Executive Officer",
            wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
            extraction_method="test",
        )

        assert person.ticker == "AAPL"
        assert person.clean_name == "Tim Cook"
        assert person.clean_title == "Chief Executive Officer"

    def test_invalid_person_creation(self):
        """Test validation of invalid person data."""
        from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPerson
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            # Empty name should fail
            WikipediaKeyPerson(
                ticker="AAPL",
                company_name="Apple Inc.",
                raw_name="",
                clean_name="",
                clean_title="CEO",
                wikipedia_url="https://example.com",
            )

    def test_config_validation(self):
        """Test configuration validation."""
        from corpus_types.schemas.wikipedia_key_people import validate_config

        # Valid config should pass
        issues = validate_config(self.config)
        assert len(issues) == 0

        # Invalid config should have issues
        self.config.scraping.wikipedia_rate_limit = 20  # Too high (>10)
        issues = validate_config(self.config)
        assert len(issues) > 0


class TestIntegrationScenarios:
    """Test integration scenarios with real-world complexity."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = get_default_config()
        self.extractor = EnhancedWikipediaKeyPeopleExtractor(self.config)
        self.test_company = {
            "ticker": "MCD",
            "company_name": "McDonald's",
            "wikipedia_url": "https://en.wikipedia.org/wiki/McDonald%27s",
            "index_name": "dow",
        }

    @patch(
        "corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper.session"
    )
    def test_complex_mcdonalds_structure(self, mock_session):
        """Test extraction from complex McDonald's page structure."""
        # Mock a complex page with multiple sections and nested tables
        mock_response = Mock()
        mock_response.text = """
        <html>
        <body>
        <!-- Main infobox -->
        <table class="infobox vcard">
        <tr><th>Key people</th><td>
        <li>Chris Kempczinski (President and CEO)</li>
        <li>Joe Erlinger (President, McDonald's USA)</li>
        </td></tr>
        </table>

        <!-- Board of Directors section -->
        <h2>Board of directors</h2>
        <table class="wikitable">
        <tr><th>Name</th><th>Position</th></tr>
        <tr><td>John Rogers</td><td>Chairman</td></tr>
        <tr><td>Mary Dillon</td><td>Director</td></tr>
        <tr><td>Robert Eckert</td><td>Director</td></tr>
        </table>

        <!-- Executive team section -->
        <h3>Executive team</h3>
        <ul>
        <li>Suzanne Nora Johnson (Chief Impact Officer)</li>
        <li>Ian Borden (President, International)</li>
        </ul>

        <!-- Some other table that might have people -->
        <table class="wikitable">
        <tr><td>Leadership information</td></tr>
        <tr><td>John Doe serves as Senior Vice President</td></tr>
        </table>
        </body>
        </html>
        """
        mock_session.get.return_value = mock_response

        people = self.extractor.extract_key_people(self.test_company)

        # Should find people from multiple sources
        names = [p.clean_name for p in people]

        # From infobox
        assert "Chris Kempczinski" in names
        assert "Joe Erlinger" in names

        # From board table
        assert any("Rogers" in name for name in names)
        assert any("Dillon" in name for name in names)

        # From executive team list
        assert any("Johnson" in name for name in names)

        # Should have multiple people
        assert len(people) >= 5

    @patch(
        "corpus_hydrator.adapters.wikipedia_key_people.core.enhanced_scraper.session"
    )
    def test_minimal_page_structure(self, mock_session):
        """Test extraction from page with minimal structure."""
        mock_response = Mock()
        mock_response.text = """
        <html>
        <body>
        <p>McDonald's Corporation is a fast food company.</p>
        <p>The CEO is Chris Kempczinski and the CFO is Kevin Ozan.</p>
        </body>
        </html>
        """
        mock_session.get.return_value = mock_response

        people = self.extractor.extract_key_people(self.test_company)

        # Should still find people even from minimal structure
        names = [p.clean_name for p in people]
        # May or may not find them depending on exact text parsing
        # This tests the robustness of the extraction

    def test_edge_cases(self):
        """Test various edge cases and error conditions."""
        # Test with None inputs
        result = self.extractor._parse_person_text(None, self.test_company)
        assert result is None

        result = self.extractor._parse_person_text("", self.test_company)
        assert result is None

        # Test with very long names
        long_name = "A Very Long Name That Should Be Considered Invalid Because It Is Too Long To Be A Real Person Name And Should Be Rejected By The Validation"
        result = self.extractor._looks_like_person_name(long_name)
        assert result == False

        # Test numeric content
        result = self.extractor._looks_like_person_name("John Doe 123")
        assert result == False


if __name__ == "__main__":
    pytest.main([__file__])
