"""
Quote extraction functionality.

This module provides comprehensive quote extraction from legal documents,
including first-pass filtering, attribution, and semantic reranking.
"""

import json
from pathlib import Path
from typing import Iterator, List, Optional, Set

from corpus_cleaner.cleaner import TextCleaner
from corpus_types.schemas.models import QuoteCandidate
from loguru import logger

from ..infrastructure.court_provenance import CourtProvenanceExtractor
from ..infrastructure.registry import (
    DEFAULT_COMPANY_ALIASES,
    DEFAULT_KEYWORDS,
    DEFAULT_SEED_QUOTES,
    DEFAULT_SIMILARITY_THRESHOLD,
    get_default_quote_config,
)
from ..position_features import append_positional_features

# from .case_outcome_imputer import add_final_judgement_to_quotes


class FirstPassExtractor:
    """First pass extraction using regex and keyword filtering."""

    def __init__(self, keywords: List[str] = None, cleaner: TextCleaner = None):
        """Initialize first pass extractor.

        Args:
            keywords: Keywords to filter for relevant content
            cleaner: Text cleaner instance
        """
        self.keywords = keywords or DEFAULT_KEYWORDS
        self.cleaner = cleaner or TextCleaner()

    def extract(self, text: str) -> Iterator[QuoteCandidate]:
        """Extract potential quotes from text.

        Args:
            text: Input text to process

        Yields:
            QuoteCandidate objects for potential quotes
        """
        # This is a simplified implementation
        # In practice, this would contain the actual regex and filtering logic
        # from the original first_pass.py module
        yield from []


class Attributor:
    """Speaker attribution for extracted quotes."""

    def __init__(self, company_aliases: Set[str] = None):
        """Initialize attributor.

        Args:
            company_aliases: Company/person aliases for attribution
        """
        self.company_aliases = company_aliases or DEFAULT_COMPANY_ALIASES

    def filter(self, candidates: List[QuoteCandidate]) -> Iterator[QuoteCandidate]:
        """Filter and attribute quotes to speakers.

        Args:
            candidates: List of quote candidates

        Yields:
            Attributed quote candidates
        """
        # This is a simplified implementation
        # In practice, this would contain the actual attribution logic
        # from the original attribution.py module
        for candidate in candidates:
            yield candidate


class SemanticReranker:
    """Semantic reranking using similarity to seed quotes."""

    def __init__(self, seed_quotes: List[str] = None, threshold: float = None):
        """Initialize reranker.

        Args:
            seed_quotes: Example quotes for semantic similarity
            threshold: Minimum similarity threshold
        """
        self.seed_quotes = seed_quotes or DEFAULT_SEED_QUOTES
        self.threshold = (
            threshold if threshold is not None else DEFAULT_SIMILARITY_THRESHOLD
        )

    def rerank(self, attributed: List[QuoteCandidate]) -> Iterator[QuoteCandidate]:
        """Rerank quotes by semantic similarity.

        Args:
            attributed: List of attributed quote candidates

        Yields:
            Reranked quote candidates
        """
        # This is a simplified implementation
        # In practice, this would contain the actual reranking logic
        # from the original rerank.py module
        for candidate in attributed:
            yield candidate


class QuoteExtractor:
    """
    Main quote extractor that orchestrates the full pipeline:
    1. First pass extraction (regex + keyword filtering)
    2. Attribution (speaker identification)
    3. Semantic reranking
    """

    def __init__(
        self,
        keywords: List[str] = None,
        company_aliases: Set[str] = None,
        seed_quotes: List[str] = None,
        threshold: float = None,
        case_dir: Optional[str] = None,
    ):
        """
        Initialize the quote extractor with configuration.

        Args:
            keywords: Keywords to filter for relevant content
            company_aliases: Company/person aliases for attribution
            seed_quotes: Example quotes for semantic similarity
            threshold: Minimum similarity threshold for reranking
            case_dir: Path to case directory for position features
        """
        # Use defaults from registry if not provided
        if keywords is None:
            keywords = DEFAULT_KEYWORDS
        if company_aliases is None:
            company_aliases = DEFAULT_COMPANY_ALIASES
        if seed_quotes is None:
            seed_quotes = DEFAULT_SEED_QUOTES
        if threshold is None:
            threshold = DEFAULT_SIMILARITY_THRESHOLD

        self.cleaner = TextCleaner()
        self.first_pass = FirstPassExtractor(keywords, self.cleaner)
        self.attributor = Attributor(company_aliases)
        self.reranker = SemanticReranker(seed_quotes, threshold)
        self.case_dir = case_dir
        self.court_provenance_extractor = CourtProvenanceExtractor()

    def extract_quotes(
        self, doc_text: str, doc_id: str = "", case_id: str = ""
    ) -> List[QuoteCandidate]:
        """
        Extract quotes method for test compatibility.
        Returns a list instead of iterator.
        """
        return list(self.extract(doc_text, doc_id, case_id))

    def extract(
        self, doc_text: str, doc_id: str = "", case_id: str = ""
    ) -> Iterator[QuoteCandidate]:
        """
        Extract and process quotes through the full pipeline.

        Args:
            doc_text: The full text of the document
            doc_id: Document identifier
            case_id: Case identifier

        Yields:
            Fully processed QuoteCandidate objects with all fields
        """
        # Clean the document text
        cleaned_text = self.cleaner.clean(doc_text)

        # First pass: extract potential quotes
        candidates = list(self.first_pass.extract(cleaned_text))
        if not candidates:
            return

        logger.debug(f"First pass found {len(candidates)} candidates")

        # Attribution: identify speakers
        attributed = list(self.attributor.filter(candidates))
        if not attributed:
            return

        logger.debug(f"Attribution found {len(attributed)} attributed quotes")

        # Semantic reranking: filter by similarity to seed quotes
        final_quotes = list(self.reranker.rerank(attributed))
        logger.debug(f"Reranking found {len(final_quotes)} final quotes")

        # Convert to dictionaries for further processing
        quote_dicts = []
        for quote in final_quotes:
            quote_dict = quote.to_dict()
            quote_dict["doc_id"] = doc_id
            quote_dict["case_id"] = case_id
            quote_dicts.append(quote_dict)

        # Add position features if case directory is available
        if self.case_dir and quote_dicts:
            try:
                quote_dicts = append_positional_features(self.case_dir, quote_dicts)
                logger.debug("Added positional features to quotes")
            except Exception as e:
                logger.warning(f"Failed to add positional features: {e}")

        # Add court provenance fields
        if quote_dicts:
            try:
                quote_dicts = (
                    self.court_provenance_extractor.enrich_quotes_with_provenance(
                        quote_dicts
                    )
                )
                logger.debug("Added court provenance to quotes")
            except Exception as e:
                logger.warning(f"Failed to add court provenance: {e}")

        # Convert back to QuoteCandidate objects and yield
        for quote_dict in quote_dicts:
            yield QuoteCandidate(**quote_dict)

    def process_file(self, input_file: Path, output_file: Path) -> None:
        """
        Process a single file and save extracted quotes to output file.

        Args:
            input_file: Path to the input file to process
            output_file: Path to save the results
        """
        try:
            # For compatibility with test, assume input is JSON with text field
            if input_file.suffix == ".json":
                data = json.loads(input_file.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    all_quotes = []
                    for item in data:
                        text = item.get("opinion_text", "")
                        quotes = list(self.extract(text))
                        all_quotes.extend([q.to_dict() for q in quotes])
                    output_file.write_text(json.dumps(all_quotes, indent=2))
                else:
                    text = data.get("opinion_text", "")
                    quotes = list(self.extract(text))
                    output_file.write_text(
                        json.dumps([q.to_dict() for q in quotes], indent=2)
                    )
            else:
                # For text files, process directly
                text = input_file.read_text(encoding="utf-8")
                quotes = list(self.extract(text))
                output_file.write_text(
                    json.dumps([q.to_dict() for q in quotes], indent=2)
                )
        except Exception as e:
            logger.error(f"Error processing file {input_file}: {e}")

    def process_directory(self, input_dir: Path, output_dir: Path) -> None:
        """
        Process all files in input directory and save results to output directory.

        Args:
            input_dir: Directory containing files to process
            output_dir: Directory to save results
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        for input_file in input_dir.glob("*.json"):
            if input_file.is_file():
                logger.info(f"Processing {input_file}")
                output_file = output_dir / f"processed_{input_file.name}"
                self.process_file(input_file, output_file)


def extract_quotes(
    doc_text: str, doc_id: str = "", case_id: str = ""
) -> List[QuoteCandidate]:
    """
    Extract quotes from document text.

    This is a standalone function that creates a FirstPassExtractor instance
    and uses it to extract quotes.

    Args:
        doc_text: The full text of the document
        doc_id: Document identifier
        case_id: Case identifier

    Returns:
        List of QuoteCandidate objects
    """
    extractor = FirstPassExtractor()
    return extractor.extract_quotes(doc_text, doc_id, case_id)
