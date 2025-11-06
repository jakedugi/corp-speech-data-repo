#!/usr/bin/env python3
"""
Minimal test script for outcome extraction functionality.
Tests just the core logic without full module imports.
"""

import json
import re
from pathlib import Path
from typing import List, NamedTuple, Dict, Any

# Copy the core classes and functions needed for testing
DEFAULT_MIN = 10000
DEFAULT_CONTEXT = 100

class Candidate(NamedTuple):
    value: float
    raw_text: str
    context: str
    feature_votes: int

class AmountSelector:
    def choose(self, candidates: List[Candidate]) -> float | None:
        if not candidates:
            return None
        sorted_candidates = sorted(
            candidates, key=lambda c: (c.feature_votes, c.value), reverse=True
        )
        return sorted_candidates[0].value

class CaseOutcomeImputer:
    def __init__(self, config: dict = None):
        self.config = config or {}

    def extract_outcomes(self, doc_data: dict) -> List[dict]:
        text = doc_data.get("raw_text", "")
        doc_id = doc_data.get("doc_id", "")
        if not text:
            return []

        outcomes = []
        candidates = self.scan_for_candidates_from_text(text, doc_id)
        selector = AmountSelector()
        best_amount = selector.choose(candidates)

        outcome_type = self._classify_outcome_type(text)
        court_info = self._extract_court_info(text)

        if best_amount or outcome_type:
            outcome = {
                "doc_id": doc_id,
                "outcome_type": outcome_type,
                "amount": best_amount,
                "court_type": court_info.get("court_type"),
                "is_dismissed": court_info.get("is_dismissed", False),
                "has_fee_shifting": court_info.get("has_fee_shifting", False),
                "candidates": [
                    {
                        "value": c.value,
                        "raw_text": c.raw_text,
                        "context": c.context,
                        "feature_votes": c.feature_votes
                    } for c in candidates[:5]
                ] if candidates else []
            }
            outcomes.append(outcome)

        return outcomes

    def scan_for_candidates_from_text(self, text: str, doc_id: str) -> List[Candidate]:
        seen = set()
        out = []
        dollar_pattern = re.compile(r'\$[\d,]+(?:\.\d{2})?')

        for match in dollar_pattern.finditer(text):
            try:
                amount_str = match.group(0).replace('$', '').replace(',', '')
                value = float(amount_str)

                if value >= DEFAULT_MIN:
                    start, end = match.span()
                    context = text[max(0, start - DEFAULT_CONTEXT):end + DEFAULT_CONTEXT].replace('\n', ' ')

                    sig = f"{value}:{context[:60]}"
                    if sig not in seen:
                        seen.add(sig)
                        feature_votes = 0
                        if 'judgment' in context.lower():
                            feature_votes += 2
                        if 'settlement' in context.lower():
                            feature_votes += 2
                        if 'penalty' in context.lower():
                            feature_votes += 1
                        if 'award' in context.lower():
                            feature_votes += 1

                        out.append(Candidate(
                            value=value,
                            raw_text=match.group(0),
                            context=context,
                            feature_votes=feature_votes
                        ))
            except ValueError:
                continue

        return sorted(out, key=lambda c: (c.feature_votes, c.value), reverse=True)

    def _classify_outcome_type(self, text: str) -> str | None:
        text_lower = text.lower()

        if 'stipulated judgment' in text_lower or 'consent judgment' in text_lower:
            return 'stipulated_judgment'
        elif 'dismissed' in text_lower and ('with prejudice' in text_lower or 'without prejudice' in text_lower):
            return 'dismissal'
        elif 'settlement' in text_lower or 'settled' in text_lower:
            return 'settlement'
        elif 'judgment' in text_lower and 'default' in text_lower:
            return 'default_judgment'
        elif 'summary judgment' in text_lower:
            return 'summary_judgment'
        elif 'jury verdict' in text_lower or 'jury found' in text_lower:
            return 'jury_verdict'
        elif 'bench trial' in text_lower or 'court finds' in text_lower:
            return 'bench_judgment'
        elif 'consent decree' in text_lower:
            return 'consent_decree'
        elif 'injunction' in text_lower and ('permanent' in text_lower or 'preliminary' in text_lower):
            return 'injunctive_relief'

        return None

    def _extract_court_info(self, text: str) -> dict:
        court_info = {
            "court_type": None,
            "is_dismissed": False,
            "has_fee_shifting": False
        }

        text_lower = text.lower()

        if 'district court' in text_lower:
            court_info["court_type"] = "district"
        elif 'court of appeals' in text_lower or 'circuit court' in text_lower:
            court_info["court_type"] = "appeals"
        elif 'supreme court' in text_lower:
            court_info["court_type"] = "supreme"
        elif 'bankruptcy court' in text_lower:
            court_info["court_type"] = "bankruptcy"

        court_info["is_dismissed"] = (
            'dismissed' in text_lower and
            ('with prejudice' in text_lower or 'without prejudice' in text_lower)
        )

        court_info["has_fee_shifting"] = (
            ('attorney' in text_lower and 'fee' in text_lower) or
            'fee shifting' in text_lower or
            'prevailing party' in text_lower
        )

        return court_info

    def extract_cash_amounts(self, text: str, doc_id: str) -> List[dict]:
        candidates = self.scan_for_candidates_from_text(text, doc_id)

        amounts = []
        for candidate in candidates[:10]:
            amounts.append({
                "value": candidate.value,
                "raw_text": candidate.raw_text,
                "context": candidate.context,
                "feature_votes": candidate.feature_votes
            })

        return amounts

def test_outcome_extraction():
    """Test outcome extraction on sample documents."""

    test_file = Path("data/test_outcomes.jsonl")
    if not test_file.exists():
        print(f"Test file {test_file} not found")
        return

    extractor = CaseOutcomeImputer()

    print("Testing outcome extraction...")
    print("=" * 50)

    with open(test_file, "r") as f:
        for i, line in enumerate(f):
            print(f"\n--- Document {i+1} ---")

            doc = json.loads(line.strip())
            doc_id = doc.get("doc_id", f"doc_{i}")

            print(f"Doc ID: {doc_id}")

            outcomes = extractor.extract_outcomes(doc)

            if outcomes:
                for outcome in outcomes:
                    print(f"Outcome Type: {outcome.get('outcome_type')}")
                    print(f"Amount: {outcome.get('amount')}")
                    print(f"Court Type: {outcome.get('court_type')}")
                    print(f"Is Dismissed: {outcome.get('is_dismissed')}")
                    print(f"Has Fee Shifting: {outcome.get('has_fee_shifting')}")
                    print(f"Number of candidates: {len(outcome.get('candidates', []))}")
                    if outcome.get('candidates'):
                        top_candidate = outcome['candidates'][0]
                        print(f"Top candidate: ${top_candidate['value']:,.0f} (votes: {top_candidate['feature_votes']})")
                        print(f"Context: ...{top_candidate['context'][:100]}...")
            else:
                print("No outcomes extracted")

    print("\n" + "=" * 50)
    print("Testing cash amounts extraction...")
    print("=" * 50)

    with open(test_file, "r") as f:
        docs = [json.loads(line.strip()) for line in f]

    if len(docs) >= 3:
        doc = docs[2]  # The judgment document with $455,000
        doc_id = doc.get("doc_id", "doc_2")
        text = doc.get("raw_text", "")

        print(f"\n--- Cash Amounts for {doc_id} ---")

        amounts = extractor.extract_cash_amounts(text, doc_id)

        for amount in amounts[:5]:  # Show top 5
            print(f"Amount: ${amount['value']:,.0f}")
            print(f"Raw Text: {amount['raw_text']}")
            print(f"Feature Votes: {amount['feature_votes']}")
            print(f"Context: ...{amount['context'][:150]}...")
            print()

if __name__ == "__main__":
    test_outcome_extraction()
