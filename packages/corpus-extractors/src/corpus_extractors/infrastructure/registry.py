"""
Configuration constants and utilities for corpus extractors.

This module provides default configuration values and utilities.
Data models are imported from corpus_types for SSOT compliance.
"""

from typing import Dict, Any


# Default configuration values for quote extraction
DEFAULT_KEYWORDS = [
    "regulation", "policy", "statement", "violation", "compliance",
    "alleged", "claimed", "stated", "according", "reported"
]

DEFAULT_COMPANY_ALIASES = {
    "company", "corporation", "inc", "llc", "ltd", "corp",
    "corporation", "incorporated", "limited", "co"
}

DEFAULT_SEED_QUOTES = [
    "The company stated that",
    "According to the policy",
    "The corporation claimed",
    "As stated in the complaint",
    "The defendant alleged"
]

DEFAULT_SIMILARITY_THRESHOLD = 0.55


def get_default_quote_config() -> Dict[str, Any]:
    """Get default configuration for quote extraction."""
    return {
        "keywords": DEFAULT_KEYWORDS,
        "company_aliases": DEFAULT_COMPANY_ALIASES,
        "seed_quotes": DEFAULT_SEED_QUOTES,
        "threshold": DEFAULT_SIMILARITY_THRESHOLD,
    }


def get_default_outcome_config() -> Dict[str, Any]:
    """Get default configuration for outcome extraction."""
    return {
        "min_amount": 1000.0,
        "context_chars": 200,
        "min_features": 2,
        "case_position_threshold": 0.5,
        "docket_position_threshold": 0.5,
    }
