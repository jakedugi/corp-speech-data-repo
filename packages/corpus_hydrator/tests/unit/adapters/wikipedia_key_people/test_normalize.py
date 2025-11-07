"""
Unit tests for Wikipedia Key People Normalizer

Tests cover:
- Name and title normalization
- Data deduplication
- Quality validation
- Edge case handling
"""

import pytest
from corpus_hydrator.adapters.wikipedia_key_people.normalize import (
    WikipediaKeyPeopleNormalizer,
)
from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPerson


class TestWikipediaKeyPeopleNormalizer:
    """Test WikipediaKeyPeopleNormalizer functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.normalizer = WikipediaKeyPeopleNormalizer()

        # Create sample test data
        self.sample_people = [
            WikipediaKeyPerson(
                ticker="AAPL",
                company_name="Apple Inc.",
                raw_name="tim cook (ceo)",
                clean_name="tim cook",
                clean_title="ceo",
                wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
                extraction_method="test",
            ),
            WikipediaKeyPerson(
                ticker="AAPL",
                company_name="Apple Inc.",
                raw_name="TIM COOK — CHIEF EXECUTIVE OFFICER",
                clean_name="TIM COOK",
                clean_title="CHIEF EXECUTIVE OFFICER",
                wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
                extraction_method="test",
            ),
            WikipediaKeyPerson(
                ticker="MSFT",
                company_name="Microsoft Corporation",
                raw_name="satya nadella (CEO)",
                clean_name="satya nadella",
                clean_title="CEO",
                wikipedia_url="https://en.wikipedia.org/wiki/Microsoft",
                extraction_method="test",
            ),
        ]

    def test_normalize_name_basic(self):
        """Test basic name normalization."""
        # Test lowercase to title case
        result = self.normalizer.normalize_name("john doe")
        assert result == "John Doe"

        # Test mixed case
        result = self.normalizer.normalize_name("MARY SMITH")
        assert result == "Mary Smith"

        # Test with extra spaces
        result = self.normalizer.normalize_name("  john   doe  ")
        assert result == "John Doe"

    def test_normalize_name_special_cases(self):
        """Test name normalization with special cases."""
        # Test with particles (van, de, etc.)
        result = self.normalizer.normalize_name("john van der berg")
        assert result == "John van der Berg"

        # Test with apostrophes
        result = self.normalizer.normalize_name("o'brien")
        assert result == "O'Brien"

        # Test with hyphens
        result = self.normalizer.normalize_name("jean-pierre dubois")
        assert result == "Jean-Pierre Dubois"

    def test_normalize_title_basic(self):
        """Test basic title normalization."""
        # Test abbreviation expansion
        result = self.normalizer.normalize_title("CEO")
        assert result == "Chief Executive Officer"

        result = self.normalizer.normalize_title("CFO")
        assert result == "Chief Financial Officer"

        # Test mixed case
        result = self.normalizer.normalize_title("vice president")
        assert result == "Vice President"

    def test_normalize_title_complex(self):
        """Test complex title normalization."""
        # Test multiple abbreviations
        result = self.normalizer.normalize_title("SVP & CFO")
        assert "Senior Vice President" in result
        assert "Chief Financial Officer" in result

        # Test with extra text
        result = self.normalizer.normalize_title(
            "Executive Vice President, Global Sales"
        )
        assert result == "Executive Vice President, Global Sales"

    def test_normalize_people_integration(self):
        """Test complete people normalization."""
        result = self.normalizer.normalize_people(self.sample_people)

        assert len(result) == 3  # Should preserve all unique people

        # Check first person normalization
        person1 = result[0]
        assert person1.clean_name == "Tim Cook"
        assert person1.clean_title == "Chief Executive Officer"

        # Check second person (duplicate of first)
        person2 = result[1]
        assert person2.clean_name == "Tim Cook"
        assert person2.clean_title == "Chief Executive Officer"

    def test_remove_duplicates(self):
        """Test duplicate removal functionality."""
        # Create people with exact duplicates
        duplicate_people = [
            WikipediaKeyPerson(
                ticker="TEST",
                company_name="Test Corp",
                raw_name="John Doe (CEO)",
                clean_name="John Doe",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://example.com",
                extraction_method="test",
            ),
            WikipediaKeyPerson(
                ticker="TEST",
                company_name="Test Corp",
                raw_name="John Doe — CEO",
                clean_name="John Doe",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://example.com",
                extraction_method="test",
            ),
        ]

        # Add normalizer and normalize
        result = self.normalizer.normalize_people(duplicate_people)

        # Should only have one John Doe
        john_doe_count = sum(1 for p in result if p.clean_name == "John Doe")
        assert john_doe_count == 1

    def test_remove_duplicates_different_companies(self):
        """Test that duplicates are only removed within the same company."""
        cross_company_people = [
            WikipediaKeyPerson(
                ticker="AAPL",
                company_name="Apple Inc.",
                raw_name="John Doe (CEO)",
                clean_name="John Doe",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://example.com",
                extraction_method="test",
            ),
            WikipediaKeyPerson(
                ticker="MSFT",
                company_name="Microsoft",
                raw_name="John Doe (CEO)",
                clean_name="John Doe",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://example.com",
                extraction_method="test",
            ),
        ]

        result = self.normalizer.normalize_people(cross_company_people)

        # Should keep both (different companies)
        assert len(result) == 2
        tickers = [p.ticker for p in result]
        assert "AAPL" in tickers
        assert "MSFT" in tickers

    def test_validate_people_data(self):
        """Test data quality validation."""
        # Create mixed quality data
        mixed_quality_people = [
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
                ticker="TEST",
                company_name="Test Corp",
                raw_name="",  # Invalid name
                clean_name="",
                clean_title="CEO",
                wikipedia_url="https://example.com",
                extraction_method="test",
            ),
            WikipediaKeyPerson(
                ticker="TEST2",
                company_name="Test Corp 2",
                raw_name="John Doe (EXEC)",  # Invalid title
                clean_name="John Doe",
                clean_title="EXEC",
                wikipedia_url="https://example.com",
                extraction_method="test",
            ),
        ]

        report = self.normalizer.validate_people_data(mixed_quality_people)

        assert report["total_people"] == 3
        assert report["valid_names"] == 2  # Tim Cook and John Doe
        assert report["valid_titles"] == 1  # Only Chief Executive Officer
        assert len(report["issues"]) > 0  # Should have issues

    def test_edge_cases(self):
        """Test edge cases and error conditions."""
        # Test with None input
        result = self.normalizer.normalize_name(None)
        assert result == ""

        result = self.normalizer.normalize_title(None)
        assert result == ""

        # Test with empty people list
        result = self.normalizer.normalize_people([])
        assert result == []

        # Test with very long names/titles
        long_name = "A Very Long Name That Should Be Handled Properly Even Though It Is Quite Long"
        result = self.normalizer.normalize_name(long_name)
        assert len(result) > 10  # Should still process it

    def test_title_case_name_edge_cases(self):
        """Test name title casing edge cases."""
        # Single letter names
        result = self.normalizer.normalize_name("a b")
        assert result == "A B"

        # Names with numbers (should preserve)
        result = self.normalizer.normalize_name("John Doe 123")
        assert result == "John Doe 123"

        # Names with special characters
        result = self.normalizer.normalize_name("José María")
        assert result == "José María"

    def test_title_normalization_comprehensive(self):
        """Test comprehensive title normalization."""
        test_cases = [
            ("CEO", "Chief Executive Officer"),
            ("CFO", "Chief Financial Officer"),
            ("COO", "Chief Operating Officer"),
            ("CTO", "Chief Technology Officer"),
            ("EVP", "Executive Vice President"),
            ("SVP", "Senior Vice President"),
            ("VP", "Vice President"),
            ("Dir.", "Director"),
            ("Mgr.", "Manager"),
            ("Pres.", "President"),
            ("Exec.", "Executive"),
            ("Asst.", "Assistant"),
            ("Sr.", "Senior"),
            ("Jr.", "Junior"),
        ]

        for input_title, expected in test_cases:
            result = self.normalizer.normalize_title(input_title)
            assert (
                result == expected
            ), f"Failed to normalize '{input_title}' to '{expected}', got '{result}'"

    def test_quality_indicators(self):
        """Test quality indicator functions."""
        # Test high quality name
        assert self.normalizer._is_high_quality_name("John Smith") == True
        assert self.normalizer._is_high_quality_name("J") == False
        assert self.normalizer._is_high_quality_name("John") == False  # Only first name

        # Test valid name
        assert self.normalizer._is_valid_name("John Doe") == True
        assert self.normalizer._is_valid_name("") == False
        assert self.normalizer._is_valid_name("123") == False

        # Test valid title
        assert self.normalizer._is_valid_title("Chief Executive Officer") == True
        assert self.normalizer._is_valid_title("CEO") == True
        assert self.normalizer._is_valid_title("Executive") == False  # Too generic

    def test_deduplication_performance(self):
        """Test deduplication with larger datasets."""
        # Create a larger set with some duplicates
        people = []

        # Add some unique people
        for i in range(10):
            person = WikipediaKeyPerson(
                ticker=f"TICK{i}",
                company_name=f"Company {i}",
                raw_name=f"Person {i} (CEO)",
                clean_name=f"Person {i}",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://example.com",
                extraction_method="test",
            )
            people.append(person)

        # Add some duplicates
        for i in range(3):
            duplicate = WikipediaKeyPerson(
                ticker="TICK0",
                company_name="Company 0",
                raw_name=f"Person 0 — Chief Executive Officer",
                clean_name="Person 0",
                clean_title="Chief Executive Officer",
                wikipedia_url="https://example.com",
                extraction_method="test",
            )
            people.append(duplicate)

        result = self.normalizer.normalize_people(people)

        # Should have 10 unique people (not 13)
        assert len(result) == 10

        # Should have only one "Person 0"
        person_0_count = sum(1 for p in result if p.clean_name == "Person 0")
        assert person_0_count == 1


class TestNormalizationIntegration:
    """Test normalization in integrated scenarios."""

    def setup_method(self):
        """Set up test fixtures."""
        self.normalizer = WikipediaKeyPeopleNormalizer()

    def test_full_pipeline_normalization(self):
        """Test normalization through the full pipeline."""
        # Simulate raw data as it might come from extraction
        raw_people = [
            WikipediaKeyPerson(
                ticker="AAPL",
                company_name="apple inc.",
                raw_name="tim cook (ceo)",
                clean_name="tim cook",
                clean_title="ceo",
                wikipedia_url="https://en.wikipedia.org/wiki/Apple_Inc.",
                extraction_method="test",
                confidence_score=0.8,
            ),
            WikipediaKeyPerson(
                ticker="MSFT",
                company_name="MICROSOFT CORPORATION",
                raw_name="SATYA NADELLA — CEO & CHAIRMAN",
                clean_name="SATYA NADELLA",
                clean_title="CEO & CHAIRMAN",
                wikipedia_url="https://en.wikipedia.org/wiki/Microsoft",
                extraction_method="test",
                confidence_score=0.7,
            ),
        ]

        # Run normalization
        normalized = self.normalizer.normalize_people(raw_people)

        # Verify comprehensive normalization
        assert len(normalized) == 2

        # Check Apple person
        apple_person = next(p for p in normalized if p.ticker == "AAPL")
        assert apple_person.clean_name == "Tim Cook"
        assert apple_person.clean_title == "Chief Executive Officer"
        assert apple_person.company_name == "apple inc."  # Company name not normalized

        # Check Microsoft person
        msft_person = next(p for p in normalized if p.ticker == "MSFT")
        assert msft_person.clean_name == "Satya Nadella"
        assert "Chief Executive Officer" in msft_person.clean_title
        assert "Chairman" in msft_person.clean_title


if __name__ == "__main__":
    pytest.main([__file__])
