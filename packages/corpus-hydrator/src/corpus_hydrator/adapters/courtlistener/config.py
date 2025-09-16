"""
CourtListener Configuration

This module provides configuration utilities for the CourtListener adapter.
All configuration models are defined in corpus-types for SSOT compliance.
"""

import os
from pathlib import Path
from typing import Dict, Optional

from loguru import logger
from corpus_types.schemas import CourtListenerConfig

# Import statute queries from parsers
from .parsers.query_builder import STATUTE_QUERIES


def get_default_config() -> CourtListenerConfig:
    """Get default CourtListener configuration with environment variable overrides.

    Returns:
        CourtListenerConfig: Default configuration from corpus-types
    """
    try:
        # Load from environment variables
        api_token = os.getenv("COURTLISTENER_API_TOKEN")
        rate_limit = float(os.getenv("COURTLISTENER_RATE_LIMIT", "0.25"))
        output_dir = Path(
            os.getenv("COURTLISTENER_OUTPUT_DIR", "data/raw/courtlistener")
        )
        default_pages = int(os.getenv("COURTLISTENER_DEFAULT_PAGES", "1"))
        default_page_size = int(os.getenv("COURTLISTENER_DEFAULT_PAGE_SIZE", "50"))
        default_date_min = os.getenv("COURTLISTENER_DEFAULT_DATE_MIN")
        api_mode = os.getenv("COURTLISTENER_API_MODE", "standard")
        default_chunk_size = int(os.getenv("COURTLISTENER_CHUNK_SIZE", "10"))
        max_concurrency = int(os.getenv("COURTLISTENER_MAX_CONCURRENCY", "2"))
        async_rate_limit = float(os.getenv("COURTLISTENER_ASYNC_RATE_LIMIT", "3.0"))

        config = CourtListenerConfig(
            api_token=api_token,
            rate_limit=rate_limit,
            output_dir=output_dir,
            default_pages=default_pages,
            default_page_size=default_page_size,
            default_date_min=default_date_min,
            api_mode=api_mode,
            default_chunk_size=default_chunk_size,
            max_concurrency=max_concurrency,
            async_rate_limit=async_rate_limit,
            statute_queries=dict(STATUTE_QUERIES),  # Include statute queries as dict
        )

        # Validate API token
        if not config.api_token:
            logger.warning(
                "No API token found. Set COURTLISTENER_API_TOKEN environment variable "
                "or create a .env file with COURTLISTENER_API_TOKEN=your_token"
            )

        return config

    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


def load_config() -> CourtListenerConfig:
    """Alias for get_default_config for backward compatibility."""
    return get_default_config()
