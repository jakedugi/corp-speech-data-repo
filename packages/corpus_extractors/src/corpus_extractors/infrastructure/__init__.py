"""
Infrastructure and utility modules.

This package contains base classes, utilities, and supporting infrastructure
for the extraction pipeline including document processing, court metadata,
and outcome imputation logic.
"""

from .base_extractor import BaseExtractor
from .case_outcome_imputer import (
    AmountSelector,
    ManualAmountSelector,
    impute_for_case,
    scan_stage1,
)
from .court_provenance import CourtProvenanceExtractor
from .process_documents import DocumentProcessor

# from .registry import ComponentRegistry  # Class not defined

__all__ = [
    "BaseExtractor",
    "DocumentProcessor",
    "CourtProvenanceExtractor",
    "scan_stage1",
    "impute_for_case",
    "AmountSelector",
    "ManualAmountSelector",
]
