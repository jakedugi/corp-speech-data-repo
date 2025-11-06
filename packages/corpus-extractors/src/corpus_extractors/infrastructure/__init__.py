"""
Infrastructure and utility modules.

This package contains base classes, utilities, and supporting infrastructure
for the extraction pipeline including document processing, court metadata,
and outcome imputation logic.
"""

from .base_extractor import BaseExtractor
from .registry import ComponentRegistry
from .process_documents import process_documents
from .court_provenance import CourtProvenance
from .case_outcome_imputer import (
    scan_stage1,
    impute_for_case,
    AmountSelector,
    ManualAmountSelector,
)

__all__ = [
    "BaseExtractor",
    "ComponentRegistry",
    "process_documents",
    "CourtProvenance",
    "scan_stage1",
    "impute_for_case",
    "AmountSelector",
    "ManualAmountSelector",
]
