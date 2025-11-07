#!/usr/bin/env python3
"""
Compare original vs SEC-enhanced data quality
"""

import csv
from pathlib import Path


def compare_data_quality():
    """Compare the quality of original vs enhanced data"""
    print("Data Quality Comparison")
    print("=" * 60)

    # Load original data
    original_data = {}
    if Path("data/sp500_aliases.csv").exists():
        with open("data/sp500_aliases.csv", "r") as f:
            for row in csv.DictReader(f):
                ticker = row["ticker"]
                original_data[ticker] = row

    # Load enhanced data
    enhanced_data = {}
    if Path("data/sp500_aliases_sec_enhanced.csv").exists():
        with open("data/sp500_aliases_sec_enhanced.csv", "r") as f:
            for row in csv.DictReader(f):
                ticker = row["ticker"]
                enhanced_data[ticker] = row

    # Compare samples
    sample_tickers = ["AAPL", "MSFT", "JNJ", "WMT", "JPM"]

    print("\nData Quality Improvements:")
    print("-" * 60)

    for ticker in sample_tickers:
        if ticker in original_data and ticker in enhanced_data:
            orig = original_data[ticker]
            enh = enhanced_data[ticker]

            print(f"\n{ticker} ({orig['official_name']}):")
            print(f"   Original: {orig['official_name']}")
            print(f"   SEC Name: {enh.get('sec_entity_name', 'N/A')}")

            # Address comparison
            if enh.get("business_city"):
                print(
                    f"   Business Address: {enh['business_city']}, {enh.get('business_state', '')}"
                )

            # Industry classification
            if enh.get("sic_description"):
                print(f"   Industry: {enh['sic_code']} - {enh['sic_description']}")

            # Incorporation state
            if enh.get("state_of_incorporation"):
                print(f"   Incorporated in: {enh['state_of_incorporation']}")

            # Recent filings
            if enh.get("latest_def14a_date"):
                print(f"   Latest DEF 14A: {enh['latest_def14a_date']}")
            if enh.get("latest_10k_date"):
                print(f"   Latest 10-K: {enh['latest_10k_date']}")

    # Summary statistics
    print("\nOverall Enhancement Statistics:")
    print(f"   Total companies: {len(original_data)}")

    enhanced_count = 0
    address_count = 0
    sic_count = 0
    filing_count = 0

    for ticker, data in enhanced_data.items():
        if data.get("sec_entity_name"):
            enhanced_count += 1
        if data.get("business_city"):
            address_count += 1
        if data.get("sic_code"):
            sic_count += 1
        if data.get("latest_def14a_date") or data.get("latest_10k_date"):
            filing_count += 1

    print(
        f"   Companies with SEC entity names: {enhanced_count} ({enhanced_count/len(original_data)*100:.1f}%)"
    )
    print(
        f"   Companies with business addresses: {address_count} ({address_count/len(original_data)*100:.1f}%)"
    )
    print(
        f"   Companies with SIC classifications: {sic_count} ({sic_count/len(original_data)*100:.1f}%)"
    )
    print(
        f"   Companies with recent filings: {filing_count} ({filing_count/len(original_data)*100:.1f}%)"
    )

    print("\nKey Improvements:")
    print("   - Official SEC entity names (more accurate than Wikipedia)")
    print("   - Complete business addresses with phone numbers")
    print("   - SIC industry classifications with descriptions")
    print("   - State of incorporation")
    print("   - Fiscal year end dates")
    print("   - Recent SEC filing dates and accession numbers")
    print("   - SEC filer category (Large accelerated filer, etc.)")

    print("\nUse Cases for Enhanced Data:")
    print("   • More accurate company identification and matching")
    print("   • Industry analysis and sector classification")
    print("   • Geographic analysis by state of incorporation")
    print("   • Regulatory compliance tracking")
    print("   • Financial reporting analysis")

    print("\n" + "=" * 60)
    print("Data Quality Comparison Complete")


if __name__ == "__main__":
    compare_data_quality()
