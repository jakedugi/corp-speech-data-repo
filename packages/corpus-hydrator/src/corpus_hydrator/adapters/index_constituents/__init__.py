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

Note: Data models are defined in corpus-types for authoritative schema governance.
"""

# Core components
from .usecase import IndexExtractionUseCase, extract_index, extract_multiple_indexes

# Providers
from .providers import (
    IndexProvider,
    ProviderError,
    WikipediaProvider,
    FMPProvider,      # Placeholder
    YahooETFProvider  # Placeholder
)

# Parsers
from .parsers import (
    TableParser,
    ParserError,
    HtmlTableParser
)

# Utilities
from .normalize import normalize_row, normalize_rows
from .writer import write_bundle, to_dataframe
from .config import INDEX_CONFIGS, get_index_config, normalize_index_name

# Models (from corpus-types)
from corpus_types.schemas.models import IndexConstituent, IndexExtractionResult, IndexConstituentFilter

__all__ = [
    # Use cases
    "IndexExtractionUseCase",
    "extract_index",
    "extract_multiple_indexes",

    # Providers
    "IndexProvider",
    "ProviderError",
    "WikipediaProvider",
    "FMPProvider",      # Placeholder
    "YahooETFProvider", # Placeholder

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

    # Models (from corpus-types)
    "IndexConstituent",
    "IndexExtractionResult",
    "IndexConstituentFilter",
]
