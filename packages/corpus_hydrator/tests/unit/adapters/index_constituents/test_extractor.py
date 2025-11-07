"""
Tests for Index Constituents Use Case

Tests the core use case orchestration and integration.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest
from corpus_hydrator.adapters.index_constituents import (
    HtmlTableParser,
    IndexExtractionUseCase,
    WikipediaProvider,
    extract_index,
)
from corpus_types.schemas.models import IndexExtractionResult


class TestIndexExtractionUseCase:
    """Test the IndexExtractionUseCase class."""

    @pytest.fixture
    def mock_provider(self):
        """Create a mock provider for testing."""
        provider = Mock()
        provider.name = "mock_provider"
        provider.priority = 1
        return provider

    @pytest.fixture
    def mock_parser(self):
        """Create a mock parser for testing."""
        parser = Mock()
        parser.supported_formats = ["html"]
        return parser

    @pytest.fixture
    def usecase(self, mock_provider, mock_parser):
        """Create an IndexExtractionUseCase instance for testing."""
        return IndexExtractionUseCase(mock_provider, mock_parser)

    def test_usecase_initialization(self, usecase, mock_provider, mock_parser):
        """Test usecase initialization."""
        assert usecase.provider == mock_provider
        assert usecase.parser == mock_parser

    def test_usecase_success(self, usecase, mock_provider, mock_parser):
        """Test successful use case execution."""
        # Setup mocks
        mock_provider.fetch_raw.return_value = {
            "content": "<html><body>Test</body></html>",
            "url": "https://example.com",
            "index_key": "sp500",
            "index_name": "S&P 500",
            "source": "mock",
            "format": "html",
        }

        mock_parser.parse.return_value = [
            {"Symbol": "AAPL", "Security": "Apple Inc.", "GICS Sector": "Technology"}
        ]

        # Execute use case
        with patch(
            "corpus_hydrator.adapters.index_constituents.normalize_rows"
        ) as mock_normalize:
            mock_normalize.return_value = [
                Mock(symbol="AAPL", company_name="Apple Inc.", index_name="S&P 500")
            ]

            result = usecase.execute("sp500")

        assert result.success is True
        assert result.index_name == "S&P 500"
        assert len(result.constituents) == 1
        assert result.total_constituents == 1
        mock_provider.fetch_raw.assert_called_once_with("sp500")
        mock_parser.parse.assert_called_once()

    def test_usecase_provider_failure(self, usecase, mock_provider):
        """Test use case when provider fails."""
        from corpus_hydrator.adapters.index_constituents.providers.base import (
            ProviderError,
        )

        mock_provider.fetch_raw.side_effect = ProviderError(
            "mock", "sp500", "Network error"
        )

        result = usecase.execute("sp500")

        assert result.success is False
        assert "Network error" in result.error_message
        assert result.total_constituents == 0

    def test_usecase_parser_failure(self, usecase, mock_provider, mock_parser):
        """Test use case when parser fails."""
        from corpus_hydrator.adapters.index_constituents.parsers.base import ParserError

        mock_provider.fetch_raw.return_value = {
            "content": "<html><body>Test</body></html>",
            "url": "https://example.com",
        }
        mock_parser.parse.side_effect = ParserError("mock_parser", "Parse error")

        result = usecase.execute("sp500")

        assert result.success is False
        assert "Parse error" in result.error_message
        assert result.total_constituents == 0


class TestWikipediaProvider:
    """Test the WikipediaProvider class."""

    @pytest.fixture
    def provider(self):
        """Create a WikipediaProvider instance for testing."""
        return WikipediaProvider()

    def test_provider_initialization(self, provider):
        """Test provider initialization."""
        assert provider.name == "wikipedia"
        assert provider.priority == 1

    def test_provider_fetch_raw(self, provider):
        """Test raw data fetching."""
        # This would make a real HTTP request, so we'll just test the interface
        assert hasattr(provider, "fetch_raw")
        assert callable(provider.fetch_raw)

    def test_provider_with_cache(self):
        """Test provider with cache directory."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir) / "cache"
            provider = WikipediaProvider(cache_dir=cache_dir)

            assert provider.http_client.cache is not None

    def test_provider_force_refresh(self):
        """Test provider with force refresh."""
        provider = WikipediaProvider(force_refresh=True)
        assert provider.force_refresh is True


class TestHtmlTableParser:
    """Test the HtmlTableParser class."""

    @pytest.fixture
    def parser(self):
        """Create a HtmlTableParser instance for testing."""
        return HtmlTableParser()

    def test_parser_initialization(self, parser):
        """Test parser initialization."""
        assert parser.supported_formats == ["html"]

    def test_parser_parse_empty_content(self, parser):
        """Test parsing empty content."""
        from corpus_hydrator.adapters.index_constituents.parsers.base import ParserError

        with pytest.raises(ParserError):
            parser.parse({"content": ""})

    def test_parser_parse_missing_index_key(self, parser):
        """Test parsing without index_key."""
        from corpus_hydrator.adapters.index_constituents.parsers.base import ParserError

        with pytest.raises(ParserError):
            parser.parse({"content": "<html></html>"})


class TestExtractIndexFunction:
    """Test the extract_index convenience function."""

    def test_extract_index_success(self):
        """Test successful extraction using convenience function."""
        mock_provider = Mock()
        mock_provider.fetch_raw.return_value = {
            "content": "<html><body>Test</body></html>",
            "url": "https://example.com",
            "index_key": "sp500",
            "index_name": "S&P 500",
        }

        mock_parser = Mock()
        mock_parser.parse.return_value = [{"Symbol": "AAPL", "Security": "Apple Inc."}]

        with patch(
            "corpus_hydrator.adapters.index_constituents.normalize_rows"
        ) as mock_normalize:
            mock_normalize.return_value = [
                Mock(symbol="AAPL", company_name="Apple Inc.", index_name="S&P 500")
            ]

            result = extract_index("sp500", mock_provider, mock_parser)

        assert result.success is True
        assert result.index_name == "S&P 500"
        assert len(result.constituents) == 1
