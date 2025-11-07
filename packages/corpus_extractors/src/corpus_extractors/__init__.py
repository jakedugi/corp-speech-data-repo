"""
Corpus extractors module for quote and outcome extraction.

This module provides functionality to extract quotes and outcomes from
normalized documents, with stable ID schemes and deterministic spans.
"""

from .case_assignment import (
    assign_case_values,
    validate_case_values,
)

# Import from organized subpackages
from .extraction_pipeline import (
    Attributor,
    FirstPassExtractor,
    QuoteExtractor,
    SemanticReranker,
    extract_cash_amounts_stage1,
    extract_outcomes,
    extract_quotes,
    filter_heuristics,
    filter_speakers,
)
from .infrastructure import (
    AmountSelector,
    BaseExtractor,
    CourtProvenanceExtractor,
    DocumentProcessor,
    ManualAmountSelector,
    impute_for_case,
    scan_stage1,
)

__all__ = [
    # Extraction pipeline
    "QuoteExtractor",
    "extract_quotes",
    "extract_outcomes",
    "extract_cash_amounts_stage1",
    "FirstPassExtractor",
    "SemanticReranker",
    "Attributor",
    "filter_speakers",
    "filter_heuristics",
    # Infrastructure
    "BaseExtractor",
    "DocumentProcessor",
    "CourtProvenanceExtractor",
    "scan_stage1",
    "impute_for_case",
    "AmountSelector",
    "ManualAmountSelector",
    # Case assignment
    "assign_case_values",
    "validate_case_values",
]
