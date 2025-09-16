"""
CourtListener Adapter

A comprehensive adapter for CourtListener legal data collection and processing.
This module provides unified access to CourtListener APIs with clean architecture.

Features:
- Statute-based query building with company filtering
- Multi-step docket hydration (dockets → entries → documents)
- Async processing for document downloads
- Chunked queries to handle URL length limits
- Structured output with metadata preservation
- Clean separation of concerns (providers, parsers, core, utils)

Usage:
    from corpus_hydrator.adapters.courtlistener import CourtListenerUseCase

    usecase = CourtListenerUseCase(
        statutes=["FTC Section 5 (9th Cir.)"],
        company_file="data/sp500_constituents.csv"
    )
    usecase.run()
"""

from .config import get_default_config, load_config
from corpus_types.schemas import CourtListenerConfig
from .usecase import CourtListenerUseCase
from .writer import CourtListenerWriter

# Core components
from .core import (
    process_docket_entries,
    process_recap_fetch,
    process_and_save,
)

# Providers
from .providers import CourtListenerClient, AsyncCourtListenerClient

# Parsers
from .parsers import QueryBuilder, build_queries, STATUTE_QUERIES

__version__ = "1.0.0"

__all__ = [
    # Main use case
    "CourtListenerUseCase",
    "CourtListenerWriter",

    # Configuration
    "CourtListenerConfig",
    "get_default_config",
    "load_config",

    # Core processing functions
    "process_docket_entries",
    "process_recap_fetch",
    "process_and_save",

    # Providers
    "CourtListenerClient",
    "AsyncCourtListenerClient",

    # Parsers
    "QueryBuilder",
    "build_queries",
    "STATUTE_QUERIES",
]