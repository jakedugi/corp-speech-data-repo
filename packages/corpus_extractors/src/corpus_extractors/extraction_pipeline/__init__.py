"""
Extraction pipeline modules for quote, outcome, and cash amount extraction.

This package contains the core extraction logic and processing pipeline
for identifying and extracting quotes, outcomes, and monetary amounts from
legal documents.
"""

from .attribution import Attributor
from .extract_cash_amounts_stage1 import extract_cash_amounts_stage1
from .extract_outcomes import extract_outcomes
from .extract_quotes import extract_quotes
from .final_pass_filter import filter_heuristics, filter_speakers
from .first_pass import FirstPassExtractor
from .quote_extractor import QuoteExtractor
from .rerank import SemanticReranker

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
