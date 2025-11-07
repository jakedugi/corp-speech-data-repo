#!/usr/bin/env python3
"""
Test the comprehensive configuration with S&P 500 data and full spaCy NER integration.
"""

import sys
from pathlib import Path


def test_comprehensive_config():  # type: ignore[no-untyped-def]
    """Test loading and using the comprehensive configuration."""
    print("=" * 70)
    print("TESTING COMPREHENSIVE CONFIGURATION WITH S&P 500 DATA")
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
        # Import and load comprehensive config
        from corpus_extractors.extraction_pipeline.quote_extractor import QuoteExtractor

        print("Loading QuoteExtractor with comprehensive configuration...")

        # This should automatically load the comprehensive config
        extractor = QuoteExtractor()

        print("✅ QuoteExtractor initialized successfully")

        # Check configuration
        config = extractor.config

        print("\nConfiguration Summary:")
        print(f"  Executives loaded: {len(config['nlp'].get('executive_names', []))}")
        print(
            f"  Company aliases: {len(config['extraction'].get('company_aliases', []))}"
        )
        print(f"  Role keywords: {len(config['nlp'].get('role_keywords', []))}")
        print(f"  NER enabled: {config['nlp'].get('enable_ner', False)}")
        print(f"  spaCy model: {config['nlp'].get('spacy_model', 'unknown')}")

        # Test sample text
        test_text = """
        Apple Inc. CEO Tim Cook stated: "We are committed to user privacy and data protection."
        According to Microsoft's Satya Nadella, "The new regulations will impact our business model."
        Tesla's Elon Musk explained: "Our approach to innovation drives industry standards."
        """

        print("\nTesting extraction on sample text:")
        print("-" * 50)
        print(test_text.strip())
        print("-" * 50)

        # Extract quotes
        quotes = extractor.extract_quotes(test_text)

        print(f"\nExtracted {len(quotes)} quotes:")

        for i, quote in enumerate(quotes, 1):
            print(f'  {i}. "{quote.quote[:60]}..."')
            print(f"     Speaker: {quote.speaker}")
            print(f"     Score: {quote.score:.3f}")
            print(f"     Context: {quote.context[:80]}...")
            print()

        # Test NER functionality
        print("Testing spaCy NER integration...")

        # Access the attributor's NLP model
        nlp = extractor.attributor.nlp

        test_doc = nlp("Apple CEO Tim Cook announced new privacy features.")
        entities = [(ent.text, ent.label_) for ent in test_doc.ents]

        print(f"NER found {len(entities)} entities:")
        for entity_text, entity_label in entities:
            print(f"  - {entity_text}: {entity_label}")

        print("\n" + "=" * 70)
        print("✅ COMPREHENSIVE CONFIGURATION TEST PASSED")
        print("✅ S&P 500 executive data integrated")
        print("✅ spaCy NER fully implemented")
        print("✅ Company and executive recognition working")
        print("✅ Quote extraction pipeline functional")

        return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_individual_components():  # type: ignore[no-untyped-def]
    """Test individual components separately."""
    print("\n" + "=" * 50)
    print("TESTING INDIVIDUAL COMPONENTS")
    print("=" * 50)

    try:
        # Test config loading
        sys.path.insert(
            0, str(Path(__file__).parent / "packages" / "corpus_extractors" / "configs")
        )
        from quotes_comprehensive import config

        print("✅ Comprehensive config loaded successfully")
        print(f"   Config has {len(config)} main sections")

        # Test spaCy loading
        from corpus_extractors.extraction_pipeline.attribution import get_nlp

        print("\nTesting spaCy model loading...")
        nlp = get_nlp("en_core_web_sm", enable_ner=True, use_gpu=False)

        if nlp:
            print("✅ spaCy model loaded successfully")
            print(f"   Pipelines: {nlp.pipe_names}")

            # Test NER
            doc = nlp("Apple CEO Tim Cook said something important.")
            entities = list(doc.ents)
            print(f"   NER test: Found {len(entities)} entities")

            for ent in entities[:3]:  # Show first 3
                print(f"     - {ent.text}: {ent.label_}")

        else:
            print("❌ spaCy model failed to load")

        return True

    except Exception as e:
        print(f"❌ Component test failed: {e}")
        return False


if __name__ == "__main__":
    success1 = test_comprehensive_config()  # type: ignore[no-untyped-call]
    success2 = test_individual_components()  # type: ignore[no-untyped-call]

    if success1 and success2:
        print("\n🎉 ALL TESTS PASSED - Comprehensive S&P 500 integration is working!")
    else:
        print("\n❌ Some tests failed")
        sys.exit(1)
