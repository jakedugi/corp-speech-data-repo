"""
Corpus extractors module for quote and outcome extraction.

This module provides functionality to extract quotes and outcomes from
normalized documents, with stable ID schemes and deterministic spans.
"""

from .attribution import Attributor
from .base_extractor import BaseExtractor
from .case_outcome_imputer import (
    AmountSelector,
    ManualAmountSelector,
    impute_for_case,
    scan_stage1,
)
from .final_pass_filter import filter_heuristics, filter_speakers
from .first_pass import FirstPassExtractor
from .quote_extractor import QuoteExtractor
from .rerank import SemanticReranker

# from .extract_cash_amounts_stage1 import extract_cash_amounts_stage1  # Function not found
# from .final_evaluate import evaluate_outcomes  # Functions not found

__all__ = [
    "QuoteExtractor",
    "FirstPassExtractor",
    "Attributor",
    "SemanticReranker",
    "filter_speakers",
    "filter_heuristics",
    "BaseExtractor",
    "scan_stage1",
    "impute_for_case",
    "AmountSelector",
    "ManualAmountSelector",
    # "extract_cash_amounts_stage1",  # Function not found    # "evaluate_outcomes",  # Functions not found
]
