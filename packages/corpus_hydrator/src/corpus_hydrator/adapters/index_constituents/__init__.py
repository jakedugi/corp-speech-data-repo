"""
Index Constituents System

This module provides functionality to extract authoritative index constituents
from multiple data sources for major US market indexes (S&P 500, Dow Jones, Nasdaq 100).

The system is organized following Clean Architecture principles:
- providers/: Data source providers (Wikipedia, FMP, Yahoo ETF)
- parsers/: Data format parsers (HTML tables, JSON, etc.)
- normalize.py: Data transformation and validation
- usecase.py: Business logic orchestration
- writer.py: Output formatting and manifest generation
- config.py: Index-specific configurations
- utils/: Shared utilities (HTTP client, caching)

Note: Data models are defined in corpus_types for authoritative schema governance.
"""

# Models (from corpus_types)
from corpus_types.schemas.models import (
    IndexConstituent,
    IndexConstituentFilter,
    IndexExtractionResult,
)

from .config import INDEX_CONFIGS, get_index_config, normalize_index_name

# Utilities
from .normalize import normalize_row, normalize_rows

# Parsers
from .parsers import HtmlTableParser, ParserError, TableParser

# Providers
from .providers import FMPProvider  # Placeholder
from .providers import YahooETFProvider  # Placeholder
from .providers import (
    IndexProvider,
    ProviderError,
    WikipediaProvider,
)

# Core components
from .usecase import IndexExtractionUseCase, extract_index, extract_multiple_indexes
from .writer import to_dataframe, write_bundle

__all__ = [
    # Use cases
    "IndexExtractionUseCase",
    "extract_index",
    "extract_multiple_indexes",
    # Providers
    "IndexProvider",
    "ProviderError",
    "WikipediaProvider",
    "FMPProvider",  # Placeholder
    "YahooETFProvider",  # Placeholder
    # Parsers
    "TableParser",
    "ParserError",
    "HtmlTableParser",
    # Utilities
    "normalize_row",
    "normalize_rows",
    "write_bundle",
    "to_dataframe",
    "INDEX_CONFIGS",
    "get_index_config",
    "normalize_index_name",
    # Models (from corpus_types)
    "IndexConstituent",
    "IndexExtractionResult",
    "IndexConstituentFilter",
]
