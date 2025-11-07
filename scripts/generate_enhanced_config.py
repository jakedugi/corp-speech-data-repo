#!/usr/bin/env python3
"""
Generate enhanced quotes.yaml configuration with S&P 500 executive data.
"""

import csv
from collections import defaultdict
from pathlib import Path

import yaml


def load_sp500_executives():
    """Load S&P 500 executive data."""
    executives = defaultdict(list)
    companies = defaultdict(list)
    executive_names = set()
    company_aliases = set()

    with open("data/sp500_key_people.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = row["company_name"].strip()
            name = row["clean_name"].strip()
            title = row["clean_title"].strip()

            if name and len(name) > 2:
                executive_names.add(name)
                executives[company].append(name)

            if company and len(company) > 2:
                company_aliases.add(company.lower())

            ticker = row["ticker"].strip()
            if ticker:
                company_aliases.add(ticker.lower())

    return list(executive_names), list(company_aliases), dict(executives)


def generate_enhanced_config():
    """Generate enhanced configuration with S&P 500 data."""
    print("Loading S&P 500 executive data...")
    executive_names, company_aliases, executives_by_company = load_sp500_executives()

    # Create enhanced configuration
    config = {
        "nlp": {
            "spacy_model": "en_core_web_sm",
            "role_keywords": [
                # Standard executive titles
                "CEO",
                "CFO",
                "CTO",
                "COO",
                "President",
                "Vice President",
                "VP",
                "Officer",
                "Director",
                "Manager",
                "spokesperson",
                "representative",
                "Chairman",
                "Chairwoman",
                "Chair",
                "Chief Executive Officer",
                "Chief Financial Officer",
                "Chief Operating Officer",
                "Chief Technology Officer",
                "Executive",
                "Board Member",
                "General Counsel",
                "Secretary",
                "Treasurer",
            ]
            + sorted(list(executive_names))[:300],  # Top 300 executive names
        },
        "extraction": {
            "keywords": [
                "regulation",
                "policy",
                "statement",
                "violation",
                "compliance",
                "law",
                "rule",
                "settlement",
                "agreement",
                "corporate",
                "company",
                "executive",
                "board",
                "shareholder",
                "stakeholder",
                "governance",
                "disclosure",
                "reporting",
                "fiduciary",
                "duty",
                "responsibility",
                "oversight",
                "supervision",
                "management",
                "leadership",
            ],
            "company_aliases": sorted(list(company_aliases))[:500],  # Top 500 aliases
            "min_quote_length": 15,
            "max_quote_length": 5000,
        },
        "reranking": {
            "enabled": True,
            "model": "all-mpnet-base-v2",
            "threshold": 0.55,
            "seed_quotes": [
                "The company stated that",
                "According to the CEO",
                "The corporation announced",
                "In a statement, the executive said",
                "The board of directors noted",
                "According to company policy",
                "The spokesperson explained",
                "Corporate regulations require",
                "The executive team decided",
                "Shareholders were informed that",
                "The chairman declared",
                "Company leadership stated",
                "The CFO reported that",
                "Board members agreed that",
                "Corporate governance requires",
            ],
        },
        "filtering": {
            "heuristics": [
                {"name": "speaker_required", "enabled": True, "threshold": 0.8},
                {"name": "context_quality", "enabled": True, "threshold": 0.7},
                {
                    "name": "length_appropriate",
                    "enabled": True,
                    "min_length": 20,
                    "max_length": 5000,
                },
            ],
            "legal_filters": {
                "court_opinions_only": False,
                "include_dissents": True,
                "include_concurrences": True,
                "exclude_routine_matters": True,
            },
        },
        "performance": {"batch_size": 100, "max_workers": 4},
        "sp500_integration": {
            "enabled": True,
            "executives_loaded": len(executive_names),
            "companies_loaded": len(set(company_aliases)),
            "top_companies": list(executives_by_company.keys())[:10],
        },
    }

    return config


def main():
    """Generate and save enhanced configuration."""
    config = generate_enhanced_config()

    # Save to enhanced config file
    output_path = Path("packages/corpus_extractors/configs/quotes_enhanced.yaml")

    with open(output_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    print(f"✅ Enhanced configuration saved to {output_path}")
    print("\nConfiguration Summary:")
    print(f"  - {config['sp500_integration']['executives_loaded']} executive names")
    print(f"  - {config['sp500_integration']['companies_loaded']} company aliases")
    print(f"  - {len(config['nlp']['role_keywords'])} total speaker recognition terms")
    print(f"  - {len(config['reranking']['seed_quotes'])} seed quotes for relevance")

    print("\nTop 5 companies by executive count:")
    for company in config["sp500_integration"]["top_companies"][:5]:
        print(f"  - {company}")

    print("\nTo use this enhanced configuration:")
    print("python -m corpus_extractors.cli.extract quotes \\")
    print("    --input data/courtlistener_normalized.jsonl \\")
    print("    --output data/quotes.jsonl \\")
    print("    --config configs/quotes_enhanced.yaml")


if __name__ == "__main__":
    main()
