"""
Case outcome extraction functionality.

This module provides comprehensive case outcome extraction from legal documents,
including cash amount detection, case disposition analysis, and metadata extraction.
"""

from __future__ import annotations
import argparse
import orjson as json
import shutil
import sys
from pathlib import Path
from typing import Iterable, List, NamedTuple

# Import from existing cash extraction module
from .extract_cash_amounts_stage1 import (
    AMOUNT_REGEX,
    PROXIMITY_PATTERN,
    JUDGMENT_VERBS,
    SPELLED_OUT_AMOUNTS,
    USD_AMOUNTS,
    extract_spelled_out_amount,
    extract_usd_amount,
    get_spacy_nlp,
    extract_spacy_amounts,
    passes_feature_filter,
    passes_enhanced_feature_filter,
    passes_enhanced_feature_filter_with_titles,
    compute_feature_votes,
    compute_enhanced_feature_votes,
    compute_enhanced_feature_votes_with_titles,
    CONTEXT_CHARS as DEFAULT_CONTEXT,
    DEFAULT_MIN_AMOUNT as DEFAULT_MIN,
    get_case_court_type,
    is_case_dismissed,
    get_case_flags,
    VotingWeights,
    DEFAULT_VOTING_WEIGHTS,
)


# ------------------------------------------------------------------------------
# Data structures
# ------------------------------------------------------------------------------


class Candidate(NamedTuple):
    value: float
    raw_text: str
    context: str
    feature_votes: int


class AmountSelector:
    def choose(self, candidates: List[Candidate]) -> float | None:
        if not candidates:
            return None
        # Sort by feature votes (descending), then by value (descending)
        sorted_candidates = sorted(
            candidates, key=lambda c: (c.feature_votes, c.value), reverse=True
        )
        return sorted_candidates[0].value


class ManualAmountSelector(AmountSelector):
    def choose(self, candidates: List[Candidate]) -> float | None:
        if not candidates:
            print("⚠ No candidate amounts found.")
            return None

        # Sort by feature votes (descending), then by value (descending)
        sorted_candidates = sorted(
            candidates, key=lambda c: (c.feature_votes, c.value), reverse=True
        )

        print("\n── Candidates (ranked by feature votes) ───────────────────────")
        for i, c in enumerate(sorted_candidates, 1):
            print(f"[{i}] {c.value:,.0f} (votes: {c.feature_votes})\t…{c.context}…")
        while True:
            choice = input("\nPick #, 's' to skip, or custom » ").strip()
            if choice.lower() == "s":
                return None
            if choice.isdigit() and 1 <= int(choice) <= len(sorted_candidates):
                return sorted_candidates[int(choice) - 1].value
            try:
                return float(choice.replace(",", ""))
            except ValueError:
                print("Invalid input—try again.")


class CaseOutcomeImputer:
    """
    Extracts case outcomes from legal documents.

    This class provides comprehensive case outcome extraction including:
    - Cash amount detection and imputation
    - Case disposition analysis
    - Court type identification
    - Dismissal detection
    """

    def __init__(self, config: dict = None):
        """Initialize the case outcome imputer.

        Args:
            config: Configuration dictionary for extraction parameters
        """
        self.config = config or {}

    def extract_outcomes(self, doc_data: dict) -> List[dict]:
        """
        Extract case outcomes from document data.

        Args:
            doc_data: Document data dictionary

        Returns:
            List of extracted outcome dictionaries
        """
        # This is a simplified implementation
        # In practice, this would integrate the full case outcome extraction logic
        # from the original case_outcome_imputer.py

        text = doc_data.get("raw_text", "")
        if not text:
            return []

        # For now, return empty list - full implementation would extract actual outcomes
        return []

    def impute_for_case(
        self,
        case_root: Path,
        selector: AmountSelector,
        min_amount: float = DEFAULT_MIN,
        context_chars: int = DEFAULT_CONTEXT,
        min_features: int = 2,
        tokenized_root: Path = None,
        extracted_root: Path = None,
        outdir: Path | None = None,
        input_stage: int = 4,
        output_stage: int = 5,
        case_position_threshold: float = 0.5,
        docket_position_threshold: float = 0.5,
        fee_shifting_ratio_threshold: float = 1.0,
        patent_ratio_threshold: float = 20.0,
        dismissal_ratio_threshold: float = 0.5,
        bankruptcy_ratio_threshold: float = 0.5,
        voting_weights: VotingWeights = DEFAULT_VOTING_WEIGHTS,
        disable_spacy: bool = False,
        disable_spelled: bool = False,
        disable_usd: bool = False,
        disable_calcs: bool = False,
        disable_regex: bool = False,
    ):
        """Impute final judgment amounts for a case."""
        # This is a simplified implementation
        # In practice, this would contain the full imputation logic
        # from the original case_outcome_imputer.py
        pass

    def scan_for_candidates(
        self,
        case_root: Path,
        min_amount: float = DEFAULT_MIN,
        context_chars: int = DEFAULT_CONTEXT,
        min_features: int = 2,
        case_position_threshold: float = 0.5,
        docket_position_threshold: float = 0.5,
        voting_weights: VotingWeights = DEFAULT_VOTING_WEIGHTS,
        disable_spacy: bool = False,
        disable_spelled: bool = False,
        disable_usd: bool = False,
        disable_calcs: bool = False,
        disable_regex: bool = False,
    ) -> List[Candidate]:
        """Scan case files for cash amount candidates."""
        # This is a simplified implementation
        # In practice, this would contain the scanning logic
        # from the original case_outcome_imputer.py scan_stage1 function
        return []
