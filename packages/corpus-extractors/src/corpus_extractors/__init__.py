"""
Corpus extractors module for quote and outcome extraction.

This module provides functionality to extract quotes and outcomes from
normalized documents, with stable ID schemes and deterministic spans.
"""

# Import from organized subpackages
from .extraction_pipeline import (
    QuoteExtractor,
    extract_quotes,
    extract_outcomes,
    extract_cash_amounts_stage1,
    FirstPassExtractor,
    SemanticReranker,
    Attributor,
    filter_speakers,
    filter_heuristics,
)

from .infrastructure import (
    BaseExtractor,
    ComponentRegistry,
    process_documents,
    CourtProvenance,
    scan_stage1,
    impute_for_case,
    AmountSelector,
    ManualAmountSelector,
)

from .case_assignment import (
    assign_case_values,
    validate_case_values,
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
    "ComponentRegistry",
    "process_documents",
    "CourtProvenance",
    "scan_stage1",
    "impute_for_case",
    "AmountSelector",
    "ManualAmountSelector",

    # Case assignment
    "assign_case_values",
    "validate_case_values",
]
