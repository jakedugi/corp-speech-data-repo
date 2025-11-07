#!/usr/bin/env python3
"""
Simple test script for outcome extraction functionality.
"""

import json
import sys
from pathlib import Path

# Add the corpus_extractors package to path
sys.path.insert(
    0, str(Path(__file__).parent / "packages" / "corpus_extractors" / "src")
)
sys.path.insert(0, str(Path(__file__).parent / "packages" / "corpus_types" / "src"))
sys.path.insert(0, str(Path(__file__).parent / "packages" / "corpus_hydrator" / "src"))

from corpus_extractors.extraction_pipeline.extract_outcomes import CaseOutcomeImputer


def test_outcome_extraction():
    """Test outcome extraction on sample documents."""

    # Load test data
    test_file = Path("data/test_outcomes.jsonl")
    if not test_file.exists():
        print(f"Test file {test_file} not found")
        return

    # Initialize extractor
    extractor = CaseOutcomeImputer()

    print("Testing outcome extraction...")
    print("=" * 50)

    with open(test_file, "r") as f:
        for i, line in enumerate(f):
            print(f"\n--- Document {i+1} ---")

            doc = json.loads(line.strip())
            doc_id = doc.get("doc_id", f"doc_{i}")

            print(f"Doc ID: {doc_id}")

            # Extract outcomes
            outcomes = extractor.extract_outcomes(doc)

            if outcomes:
                for outcome in outcomes:
                    print(f"Outcome Type: {outcome.get('outcome_type')}")
                    print(f"Amount: {outcome.get('amount')}")
                    print(f"Court Type: {outcome.get('court_type')}")
                    print(f"Is Dismissed: {outcome.get('is_dismissed')}")
                    print(f"Has Fee Shifting: {outcome.get('has_fee_shifting')}")
                    print(f"Number of candidates: {len(outcome.get('candidates', []))}")
                    if outcome.get("candidates"):
                        top_candidate = outcome["candidates"][0]
                        print(
                            f"Top candidate: ${top_candidate['value']:,.0f} (votes: {top_candidate['feature_votes']})"
                        )
                        print(f"Context: ...{top_candidate['context'][:100]}...")
            else:
                print("No outcomes extracted")

    print("\n" + "=" * 50)
    print("Testing cash amounts extraction...")
    print("=" * 50)

    # Test cash amounts extraction on the third document (which has a $455,000 settlement)
    with open(test_file, "r") as f:
        docs = [json.loads(line.strip()) for line in f]

    if len(docs) >= 3:
        doc = docs[2]  # The judgment document
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
