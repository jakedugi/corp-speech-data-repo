"""
Extraction pipeline modules for quote, outcome, and cash amount extraction.

This package contains the core extraction logic and processing pipeline
for identifying and extracting quotes, outcomes, and monetary amounts from
legal documents.
"""

from .quote_extractor import QuoteExtractor
from .extract_quotes import extract_quotes
from .extract_outcomes import extract_outcomes
from .extract_cash_amounts_stage1 import extract_cash_amounts_stage1
from .first_pass import FirstPassExtractor
from .rerank import SemanticReranker
from .attribution import Attributor
from .final_pass_filter import filter_speakers, filter_heuristics

__all__ = [
    "QuoteExtractor",
    "extract_quotes",
    "extract_outcomes",
    "extract_cash_amounts_stage1",
    "FirstPassExtractor",
    "SemanticReranker",
    "Attributor",
    "filter_speakers",
    "filter_heuristics",
]
