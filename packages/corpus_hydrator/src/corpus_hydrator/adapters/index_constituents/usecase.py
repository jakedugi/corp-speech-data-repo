"""
Index Constituents Use Case Orchestrator

This module orchestrates the extraction of index constituents following
clean architecture principles. It coordinates providers, parsers, and
normalizers to produce validated IndexConstituent objects.
"""

import logging
from typing import Dict, List

from corpus_types.schemas.models import IndexExtractionResult

from .config import get_index_config
from .normalize import normalize_rows
from .parsers.base import ParserError, TableParser
from .providers.base import IndexProvider, ProviderError

logger = logging.getLogger(__name__)


class IndexExtractionUseCase:
    """
    Use case for extracting index constituents.

    This class orchestrates the entire extraction process:
    1. Fetch raw data from provider
    2. Parse data into structured rows
    3. Normalize rows into IndexConstituent objects
    4. Return validated results
    """

    def __init__(self, provider: IndexProvider, parser: TableParser):
        """
        Initialize use case with dependencies.

        Args:
            provider: Data provider (e.g., WikipediaProvider)
            parser: Data parser (e.g., HtmlTableParser)
        """
        self.provider = provider
        self.parser = parser

    def execute(self, index_key: str) -> IndexExtractionResult:
        """
        Execute the index extraction use case.

        Args:
            index_key: Index identifier (e.g., 'sp500', 'dow', 'nasdaq100')

        Returns:
            IndexExtractionResult with constituents or error information
        """
        try:
            logger.info(f"Starting extraction for {index_key}")

            # Get index configuration
            config = get_index_config(index_key)

            # Step 1: Fetch raw data from provider
            logger.debug(f"Fetching raw data using {self.provider.name}")
            raw_data = self.provider.fetch_raw(index_key)

            # Step 2: Parse raw data into structured rows
            logger.debug(f"Parsing data using {type(self.parser).__name__}")
            rows = list(self.parser.parse(raw_data))

            if not rows:
                logger.warning(f"No rows parsed for {index_key}")
                return IndexExtractionResult(
                    index_name=config.name,
                    total_constituents=0,
                    success=False,
                    error_message="No data parsed from source",
                )

            # Step 3: Normalize rows into IndexConstituent objects
            logger.debug(f"Normalizing {len(rows)} rows")
            constituents = normalize_rows(rows, index_key, config.name)

            if not constituents:
                logger.warning(f"No constituents normalized for {index_key}")
                return IndexExtractionResult(
                    index_name=config.name,
                    total_constituents=0,
                    success=False,
                    error_message="Failed to normalize any constituents",
                )

            # Step 4: Return successful result
            logger.info(
                f"Successfully extracted {len(constituents)} constituents for {config.name}"
            )
            return IndexExtractionResult(
                index_name=config.name,
                total_constituents=len(constituents),
                constituents=constituents,
                success=True,
            )

        except (ProviderError, ParserError) as e:
            logger.error(f"Extraction failed for {index_key}: {e}")
            config = get_index_config(index_key)
            return IndexExtractionResult(
                index_name=config.name,
                total_constituents=0,
                success=False,
                error_message=str(e),
            )

        except Exception as e:
            logger.error(f"Unexpected error during extraction for {index_key}: {e}")
            config = get_index_config(index_key)
            return IndexExtractionResult(
                index_name=config.name,
                total_constituents=0,
                success=False,
                error_message=f"Unexpected error: {str(e)}",
            )


def extract_index(
    index_key: str, provider: IndexProvider, parser: TableParser
) -> IndexExtractionResult:
    """
    Convenience function for single index extraction.

    Args:
        index_key: Index identifier
        provider: Data provider
        parser: Data parser

    Returns:
        Extraction result
    """
    usecase = IndexExtractionUseCase(provider, parser)
    return usecase.execute(index_key)


def extract_multiple_indexes(
    index_keys: List[str], provider: IndexProvider, parser: TableParser
) -> Dict[str, IndexExtractionResult]:
    """
    Extract multiple indexes.

    Args:
        index_keys: List of index identifiers
        provider: Data provider
        parser: Data parser

    Returns:
        Dictionary mapping index keys to extraction results
    """
    results = {}
    for index_key in index_keys:
        logger.info(f"Processing {index_key}")
        result = extract_index(index_key, provider, parser)
        results[index_key] = result

    return results
