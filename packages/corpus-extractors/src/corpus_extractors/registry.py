"""
Model registry and configuration for corpus extractors.

This module provides model definitions, default configurations, and
registry functionality for the extraction components.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class Document:
    """Document data structure."""
    doc_id: str
    text: str
    source_path: str


@dataclass
class QuoteCandidate:
    """Quote candidate data structure."""
    quote: str
    context: str
    urls: List[str] = field(default_factory=list)
    speaker: Optional[str] = None
    score: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "quote": self.quote,
            "context": self.context,
            "urls": self.urls,
            "speaker": self.speaker,
            "score": self.score,
        }


# Default configuration values
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
