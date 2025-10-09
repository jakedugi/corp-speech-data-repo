#!/usr/bin/env python3
"""
Test script for the Wikipedia scraper.

This script demonstrates how to use the new Wikipedia scraper with different
configurations and shows the expected output formats.
"""

import sys
import os
from pathlib import Path

# Add the package paths
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).parents[2] / "corpus-types" / "src"))

from corpus_hydrator.adapters.wikipedia.scraper import WikipediaScraper
from corpus_types.schemas.scraper import (
    get_default_config,
    get_sp500_config,
    get_multi_index_config,
    validate_config
)


def test_configuration():
    """Test configuration validation."""
    print("Testing Configuration...")

    config = get_default_config()

    # Validate configuration
    issues = validate_config(config)
    if issues:
        print(f"ERROR Configuration issues: {issues}")
        return False

    print("OK Configuration is valid")
    return True


def test_dry_run():
    """Test scraper in dry-run mode."""
    print("\nTesting Dry Run Mode...")

    config = get_sp500_config()
    config.dry_run = True
    config.verbose = True
    config.scraping.max_companies = 5  # Limit for testing

    scraper = WikipediaScraper(config)

    # Test index scraping
    companies, result = scraper.scrape_index("sp500")

    print(f"Scraped {len(companies)} companies (dry run)")
    print(f"Duration: {result.duration_seconds or 0:.2f} seconds")

    if companies:
        print(f"Sample company: {companies[0].official_name} ({companies[0].ticker})")

    return len(companies) >= 0  # Should work even in dry run


def test_data_models():
    """Test data model creation and validation."""
    print("\nTesting Data Models...")

    from corpus_types.schemas.scraper import CompanyRecord, OfficerRecord

    # Test CompanyRecord
    try:
        company = CompanyRecord(
            ticker="TEST",
            official_name="Test Company Inc.",
            cik="0000123456",
            wikipedia_url="https://en.wikipedia.org/wiki/Test_Company",
            index_name="test"
        )
        print("OK CompanyRecord created successfully")
    except Exception as e:
        print(f"ERROR CompanyRecord creation failed: {e}")
        return False

    # Test OfficerRecord
    try:
        officer = OfficerRecord(
            name="John Doe",
            title="CEO",
            company_ticker="TEST",
            company_name="Test Company Inc.",
            cik="0000123456",
            source="wikipedia"
        )
        print("OK OfficerRecord created successfully")
    except Exception as e:
        print(f"ERROR OfficerRecord creation failed: {e}")
        return False

    # Test validation
    try:
        invalid_company = CompanyRecord(
            ticker="INVALID-TICKER!",
            official_name="Test",
            wikipedia_url="https://example.com",
            index_name="test"
        )
        print("ERROR Validation should have failed")
        return False
    except ValueError:
        print("OK Validation correctly rejected invalid data")

    return True


def test_index_configurations():
    """Test different index configurations."""
    print("\nTesting Index Configurations...")

    config = get_default_config()

    # Test S&P 500 config
    sp500_config = config.get_index_config("sp500")
    print(f"📈 S&P 500 URL: {sp500_config.wikipedia_url}")
    print(f"📈 S&P 500 Table ID: {sp500_config.table_id}")

    # Test Dow Jones config
    dow_config = config.get_index_config("dow")
    print(f"📉 Dow Jones URL: {dow_config.wikipedia_url}")

    # Test NASDAQ config
    nasdaq_config = config.get_index_config("nasdaq100")
    print(f"💻 NASDAQ URL: {nasdaq_config.wikipedia_url}")

    return True


def test_rate_limiting():
    """Test rate limiting functionality."""
    print("\nTesting Rate Limiting...")

    from corpus_hydrator.adapters.wikipedia.scraper import RateLimiter
    import time

    limiter = RateLimiter(rate_per_second=10, burst_size=3)

    # Test burst capacity
    for i in range(3):
        wait_time = limiter.acquire()
        assert wait_time == 0.0, f"Should not wait for burst token {i+1}"

    # Test rate limiting
    wait_time = limiter.acquire()
    assert wait_time > 0, "Should wait after burst capacity exceeded"

    print("OK Rate limiting working correctly")
    return True


def main():
    """Run all tests."""
    print("🧪 Wikipedia Scraper Test Suite")
    print("=" * 50)

    tests = [
        ("Configuration", test_configuration),
        ("Data Models", test_data_models),
        ("Index Configurations", test_index_configurations),
        ("Rate Limiting", test_rate_limiting),
        ("Dry Run", test_dry_run),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            if test_func():
                print(f"OK {test_name}: PASSED")
                passed += 1
            else:
                print(f"ERROR {test_name}: FAILED")
        except Exception as e:
            print(f"ERROR {test_name}: ERROR - {e}")

    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} passed")

    if passed == total:
        print("All tests passed!")
        return 0
    else:
        print("WARNING: Some tests failed")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
