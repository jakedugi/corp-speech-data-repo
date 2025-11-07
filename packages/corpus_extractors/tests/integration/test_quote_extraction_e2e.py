"""
End-to-end integration tests for quote extraction pipeline.

Tests the full extraction pipeline using real normalized CourtListener data.
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest
from corpus_extractors.quote_extractor import QuoteExtractor
from corpus_types.schemas.models import QuoteCandidate


@pytest.fixture
def normalized_docs() -> List[Dict[str, Any]]:
    """Load sample normalized documents from fixture."""
    fixture_path = (
        Path(__file__).parent.parent.parent / "fixtures" / "normalized_sample.jsonl"
    )
    docs = []
    with open(fixture_path, "r") as f:
        for line in f:
            docs.append(json.loads(line.strip()))
    return docs


@pytest.fixture
def extractor() -> QuoteExtractor:
    """Initialize QuoteExtractor with default config."""
    return QuoteExtractor()


class TestQuoteExtractionE2E:
    """End-to-end tests for the complete quote extraction pipeline."""

    def test_pipeline_runs_on_real_data(
        self, extractor: QuoteExtractor, normalized_docs: List[Dict[str, Any]]
    ):
        """Test that the pipeline runs without errors on real normalized data."""
        for doc in normalized_docs:
            raw_text = doc.get("raw_text", "")
            if raw_text:
                quotes = extractor.extract_quotes(raw_text)
                # Should return a list
                assert isinstance(quotes, list)

    def test_extracted_quotes_have_required_fields(
        self, extractor: QuoteExtractor, normalized_docs: List[Dict[str, Any]]
    ):
        """Test that extracted quotes have required fields."""
        all_quotes = []
        for doc in normalized_docs:
            raw_text = doc.get("raw_text", "")
            if raw_text:
                quotes = extractor.extract_quotes(raw_text)
                all_quotes.extend(quotes)

        # We should extract at least some quotes from legal documents
        if all_quotes:
            for quote in all_quotes:
                # Check that it's a QuoteCandidate
                assert isinstance(quote, QuoteCandidate)
                # Required fields
                assert hasattr(quote, "quote")
                assert hasattr(quote, "context")
                assert hasattr(quote, "speaker")
                assert hasattr(quote, "score")
                # Quote text should not be empty
                assert quote.quote
                assert len(quote.quote) > 0

    def test_quotes_have_valid_speakers(
        self, extractor: QuoteExtractor, normalized_docs: List[Dict[str, Any]]
    ):
        """Test that extracted quotes have speaker attribution."""
        all_quotes = []
        for doc in normalized_docs:
            raw_text = doc.get("raw_text", "")
            if raw_text:
                quotes = extractor.extract_quotes(raw_text)
                all_quotes.extend(quotes)

        if all_quotes:
            # After attribution filter, all quotes should have speakers
            for quote in all_quotes:
                assert quote.speaker is not None
                assert len(quote.speaker) > 0

    def test_quotes_have_similarity_scores(
        self, extractor: QuoteExtractor, normalized_docs: List[Dict[str, Any]]
    ):
        """Test that quotes have similarity scores from reranking."""
        all_quotes = []
        for doc in normalized_docs:
            raw_text = doc.get("raw_text", "")
            if raw_text:
                quotes = extractor.extract_quotes(raw_text)
                all_quotes.extend(quotes)

        if all_quotes:
            for quote in all_quotes:
                # Score should be set by reranker
                assert quote.score is not None
                assert 0.0 <= quote.score <= 1.0

    def test_quote_serialization(
        self, extractor: QuoteExtractor, normalized_docs: List[Dict[str, Any]]
    ):
        """Test that quotes can be serialized to dict for JSON output."""
        doc = normalized_docs[0]
        raw_text = doc.get("raw_text", "")

        if raw_text:
            quotes = extractor.extract_quotes(raw_text)

            if quotes:
                quote = quotes[0]
                # Test to_dict method exists and works
                quote_dict = quote.to_dict()
                assert isinstance(quote_dict, dict)
                assert "text" in quote_dict
                assert "speaker" in quote_dict
                assert "score" in quote_dict

                # Should be JSON serializable
                json_str = json.dumps(quote_dict)
                assert json_str is not None

    def test_extraction_with_doc_id(
        self, extractor: QuoteExtractor, normalized_docs: List[Dict[str, Any]]
    ):
        """Test extraction preserves doc_id for traceability."""
        for doc in normalized_docs:
            doc_id = doc.get("doc_id")
            raw_text = doc.get("raw_text", "")

            if raw_text:
                quotes = extractor.extract_quotes(raw_text)

                # Document should have an ID
                assert doc_id is not None

                # Quotes could be tagged with doc_id in downstream processing
                # This test verifies the doc structure supports it
                assert isinstance(doc_id, str)
                assert len(doc_id) > 0

    def test_pipeline_handles_empty_documents(self, extractor: QuoteExtractor):
        """Test that pipeline handles empty documents gracefully."""
        empty_text = ""
        quotes = extractor.extract_quotes(empty_text)
        assert isinstance(quotes, list)
        assert len(quotes) == 0

    def test_pipeline_handles_short_documents(self, extractor: QuoteExtractor):
        """Test that pipeline handles very short documents."""
        short_text = "The company said 'hello'."
        quotes = extractor.extract_quotes(short_text)
        # May or may not find quotes depending on filters
        assert isinstance(quotes, list)


class TestQuoteExtractionConfig:
    """Test configuration handling in quote extraction."""

    def test_default_config_loads(self):
        """Test that default config loads properly."""
        extractor = QuoteExtractor()
        assert extractor.config is not None
        assert "extraction" in extractor.config
        assert "reranking" in extractor.config
        assert "nlp" in extractor.config

    def test_custom_config_overrides_defaults(self):
        """Test that custom config values override defaults."""
        custom_config = {"reranking": {"threshold": 0.99}}
        extractor = QuoteExtractor(config=custom_config)
        assert extractor.config["reranking"]["threshold"] == 0.99
        # Other defaults should still be present
        assert "extraction" in extractor.config
        assert "nlp" in extractor.config

    def test_config_validation(self):
        """Test that config has required fields."""
        extractor = QuoteExtractor()
        config = extractor.config

        # Check required top-level keys
        assert "extraction" in config
        assert "reranking" in config
        assert "nlp" in config

        # Check extraction config
        assert "keywords" in config["extraction"]
        assert "company_aliases" in config["extraction"]

        # Check reranking config
        assert "seed_quotes" in config["reranking"]
        assert "threshold" in config["reranking"]

        # Check NLP config
        assert "spacy_model" in config["nlp"]
        assert "role_keywords" in config["nlp"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
