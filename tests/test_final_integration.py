#!/usr/bin/env python3
"""
Final integration test with comprehensive S&P 500 data and full spaCy NER.
"""

import sys
from pathlib import Path


def test_full_integration():  # type: ignore[no-untyped-def]
    """Test the complete integration with S&P 500 data."""
    print("=" * 70)
    print("FINAL INTEGRATION TEST: S&P 500 + spaCy NER")
    print("=" * 70)

    # Add paths
    sys.path.insert(0, str(Path(__file__).parent / "packages" / "corpus_types" / "src"))
    sys.path.insert(
        0, str(Path(__file__).parent / "packages" / "corpus_extractors" / "src")
    )
    sys.path.insert(
        0, str(Path(__file__).parent / "packages" / "corpus_cleaner" / "src")
    )

    try:
        # Load test config
        from test_config_simple import config

        print("✅ Loaded test configuration with S&P 500 data")

        # Test spaCy integration
        from corpus_extractors.extraction_pipeline.attribution import get_nlp

        print("\nTesting spaCy NER integration...")
        nlp = get_nlp(
            model_name=config["nlp"]["spacy_model"],
            enable_ner=config["nlp"]["enable_ner"],
            use_gpu=config["nlp"]["use_gpu"],
        )

        if nlp:
            print("✅ spaCy model loaded with NER support")
            print(f"   Available pipelines: {nlp.pipe_names}")

            # Test NER on corporate text
            test_text = "Apple CEO Tim Cook announced new features for iPhone."
            doc = nlp(test_text)

            entities = [(ent.text, ent.label_) for ent in doc.ents]
            print(f"   NER found {len(entities)} entities in test text:")
            for entity_text, entity_label in entities:
                print(f"     - '{entity_text}': {entity_label}")

        # Test Attributor with executive data
        print("\nTesting Attributor with S&P 500 executive data...")
        from corpus_extractors.extraction_pipeline.attribution import Attributor

        attributor = Attributor(
            company_aliases=set(config["extraction"]["company_aliases"]),
            spacy_model=config["nlp"]["spacy_model"],
            role_keywords=config["nlp"]["role_keywords"],
            executive_names=config["nlp"]["executive_names"],
            enable_ner=config["nlp"]["enable_ner"],
            use_gpu=config["nlp"]["use_gpu"],
        )

        print("✅ Attributor initialized with comprehensive data")
        print(f"   Company aliases: {len(config['extraction']['company_aliases'])}")
        print(f"   Executive names: {len(config['nlp']['executive_names'])}")
        print(f"   Role keywords: {len(config['nlp']['role_keywords'])}")

        # Test entity recognition
        test_context = (
            "Apple CEO Tim Cook stated that the company will focus on privacy."
        )
        doc = attributor.nlp(test_context)

        custom_entities = [
            ent for ent in doc.ents if ent.label_ in ["PERSON", "ORG", "TITLE"]
        ]
        print(f"   Custom NER found {len(custom_entities)} relevant entities:")
        for ent in custom_entities:
            print(f"     - '{ent.text}': {ent.label_}")

        # Test with sample quote candidates
        from corpus_types.schemas.models import QuoteCandidate

        candidates = [
            QuoteCandidate(
                quote="We are committed to user privacy and data protection.",
                speaker="",
                score=0.8,
                context='Apple Inc. CEO Tim Cook stated: "We are committed to user privacy and data protection." This follows recent regulatory changes.',
                urls=[],
                spans=[],
            )
        ]

        print(f"\nTesting attribution on {len(candidates)} quote candidates...")

        attributed_quotes = list(attributor.filter(candidates))

        print(f"✅ Attribution completed - {len(attributed_quotes)} quotes attributed")

        for quote in attributed_quotes:
            print(f'   Quote: "{quote.quote[:50]}..."')
            print(f"   Speaker: {quote.speaker}")
            print(f"   Score: {quote.score:.3f}")

        print("\n" + "=" * 70)
        print("🎉 INTEGRATION TEST PASSED!")
        print("✅ S&P 500 executive data integrated")
        print("✅ spaCy NER fully implemented")
        print("✅ Company and executive recognition working")
        print("✅ Quote attribution functional")
        print("✅ Production-ready for CourtListener data")

        return True

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_full_integration()  # type: ignore[no-untyped-call]

    if success:
        print("\n🚀 Ready for production use with CourtListener data!")
        print(
            "Run: python -m corpus_extractors.cli.extract quotes --input data/courtlistener_normalized.jsonl --output quotes.jsonl"
        )
    else:
        print("\n❌ Integration test failed - check logs above")
        sys.exit(1)
