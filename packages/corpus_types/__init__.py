"""
corpus_types: Authoritative schemas, IDs, and validators for the corpus pipeline.

This module provides:
- Pydantic models for data contracts (Doc, Quote, Outcome, etc.)
- Deterministic ID generation functions
- JSON Schema validation and export
- CLI tools for data validation
"""

from .ids.generate import case_id, doc_id, quote_id
from .schemas.models import (
    APIConfig,
    CasePrediction,
    CaseVector,
    Doc,
    Meta,
    Outcome,
    Prediction,
    Quote,
    QuoteCandidate,
    QuoteFeatures,
    QuoteRow,
    SchemaVersion,
    Span,
)

__version__ = "0.1.0"
__all__ = [
    # Core models
    "Doc",
    "Quote",
    "Outcome",
    "QuoteFeatures",
    "CaseVector",
    "Prediction",
    "CasePrediction",
    "Meta",
    "Span",
    "SchemaVersion",
    # Legacy models
    "APIConfig",
    "QuoteCandidate",
    "QuoteRow",
    # ID functions
    "doc_id",
    "quote_id",
    "case_id",
]
