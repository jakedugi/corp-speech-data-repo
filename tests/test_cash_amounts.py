#!/usr/bin/env python3
"""
Test script for cash amounts extraction functionality.
"""

import json
import re
from pathlib import Path
from typing import List, NamedTuple

DEFAULT_MIN = 10000
DEFAULT_CONTEXT = 100

class Candidate(NamedTuple):
    value: float
    raw_text: str
    context: str
    feature_votes: int

class CaseOutcomeImputer:
    def __init__(self, config: dict = None):
        self.config = config or {}

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

def test_cash_amounts():
    """Test cash amounts extraction on various documents."""

    # Test on courtlistener data
    test_file = Path("data/courtlistener_normalized.jsonl")
    if not test_file.exists():
        print(f"Test file {test_file} not found")
        return

    extractor = CaseOutcomeImputer()

    print("Testing cash amounts extraction on court documents...")
    print("=" * 70)

    total_amounts = 0
    docs_with_amounts = 0

    with open(test_file, "r") as f:
        for i, line in enumerate(f):
            if i >= 10:  # Test first 10 documents
                break

            doc = json.loads(line.strip())
            doc_id = doc.get("doc_id", f"doc_{i}")
            text = doc.get("raw_text", "")

            amounts = extractor.extract_cash_amounts(text, doc_id)

            if amounts:
                docs_with_amounts += 1
                total_amounts += len(amounts)

                print(f"\n--- Document {i+1}: {doc_id} ---")
                print(f"Found {len(amounts)} cash amounts:")

                for j, amount in enumerate(amounts, 1):
                    print(f"  {j}. ${amount['value']:,.0f} (votes: {amount['feature_votes']})")
                    print(f"     Raw: {amount['raw_text']}")
                    print(f"     Context: ...{amount['context'][:120]}...")
            elif i < 5:  # Show first few docs without amounts for context
                print(f"\n--- Document {i+1}: {doc_id} ---")
                print("No cash amounts found")

    print(f"\n{'='*70}")
    print(f"Summary: {docs_with_amounts} documents with amounts out of {min(10, i+1)} tested")
    print(f"Total amounts extracted: {total_amounts}")

    # Test on some specific text snippets
    print(f"\n{'='*70}")
    print("Testing specific text snippets...")

    test_texts = [
        "The court ordered defendant to pay $250,000 in damages and $50,000 in attorney fees.",
        "Plaintiff was awarded $1,500,000 in compensatory damages plus $500,000 in punitive damages.",
        "The settlement agreement requires payment of $750,000 to the plaintiff.",
        "Defendant agrees to pay a civil penalty of $100,000 for violations of the FTC Act.",
    ]

    for i, text in enumerate(test_texts, 1):
        print(f"\n--- Test Text {i} ---")
        amounts = extractor.extract_cash_amounts(text, f"test_{i}")
        for amount in amounts:
            print(f"  ${amount['value']:,.0f} (votes: {amount['feature_votes']})")
            print(f"     Context: {amount['context']}")

if __name__ == "__main__":
    test_cash_amounts()
