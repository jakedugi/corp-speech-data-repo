#!/usr/bin/env python3
"""
Update corpus_extractors configuration with comprehensive S&P 500 and Dow Jones data.
"""

import csv
from collections import defaultdict
from pathlib import Path

import yaml


def load_csv_data(file_path):
    """Load CSV data from file."""
    data = []
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data


def extract_entities():
    """Extract all unique entities from S&P 500 and Dow Jones data."""
    print("Loading S&P 500 and Dow Jones corporate data...")

    # Load all data files
    data_files = [
        "data/sp500_key_people_people.csv",
        "data/sp500_key_people_companies.csv",
        "data/sp500_key_people_appointments.csv",
        "data/sp500_key_people_roles.csv",
        "data/dow_key_people_people.csv",
        "data/dow_key_people_companies.csv",
        "data/dow_key_people_appointments.csv",
        "data/dow_key_people_roles.csv",
    ]

    people_data = []
    companies_data = []
    appointments_data = []
    roles_data = []

    for file_path in data_files:
        if Path(file_path).exists():
            data = load_csv_data(file_path)
            if "people" in file_path:
                people_data.extend(data)
            elif "companies" in file_path:
                companies_data.extend(data)
            elif "appointments" in file_path:
                appointments_data.extend(data)
            elif "roles" in file_path:
                roles_data.extend(data)

    print(f"Loaded {len(people_data)} people records")
    print(f"Loaded {len(companies_data)} company records")
    print(f"Loaded {len(appointments_data)} appointment records")
    print(f"Loaded {len(roles_data)} role records")

    # Extract unique entities
    executives = set()
    companies = set()
    company_aliases = set()
    roles = set()

    # Process people data
    for person in people_data:
        name = person.get("normalized_name", "").strip()
        full_name = person.get("full_name", "").strip()

        # Add normalized name
        if name and len(name) > 2:
            executives.add(name)

        # Add full name (cleaned)
        if full_name and len(full_name) > 2:
            # Clean up bracketed content and extra info
            clean_name = full_name.split("[")[0].split("(")[0].strip()
            if len(clean_name) > 2:
                executives.add(clean_name)

    # Process companies data
    for company in companies_data:
        company_name = company.get("company_name", "").strip()
        ticker = company.get("ticker", "").strip()

        if company_name and len(company_name) > 2:
            companies.add(company_name.lower())
            company_aliases.add(company_name.lower())

        if ticker and len(ticker) >= 2:
            company_aliases.add(ticker.lower())

    # Process roles data
    for role in roles_data:
        role_canon = role.get("role_canon", "").strip()
        role_raw = role.get("role_raw", "").strip()

        if role_canon and len(role_canon) > 2:
            roles.add(role_canon.lower())

        if role_raw and len(role_raw) > 2:
            roles.add(role_raw.lower())

    # Add standard executive titles
    standard_titles = [
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
        "Founder",
        "Co-Founder",
        "Managing Director",
        "Partner",
        "Principal",
    ]
    for title in standard_titles:
        roles.add(title.lower())

    # Clean up data - remove obviously wrong entries
    executives = {
        name
        for name in executives
        if len(name) > 3
        and not name.isupper()
        and name not in ["chair", "CHRO", "CEO", "CFO", "COO", "CTO"]
    }
    companies = {name for name in companies if len(name) > 2 and not name.isdigit()}

    print("\nExtracted entities:")
    print(f"  Executives: {len(executives)} unique")
    print(f"  Companies: {len(companies)} unique")
    print(f"  Company aliases: {len(company_aliases)} unique")
    print(f"  Roles: {len(roles)} unique")

    return (
        sorted(list(executives)),
        sorted(list(companies)),
        sorted(list(company_aliases)),
        sorted(list(roles)),
    )


def create_comprehensive_config(executives, companies, company_aliases, roles):
    """Create comprehensive configuration with all extracted data."""

    # Create enhanced configuration
    config = {
        "nlp": {
            "spacy_model": "en_core_web_sm",
            "fallback_model": "en_core_web_sm",  # Fallback if model not available
            "use_gpu": False,
            "role_keywords": roles[:500],  # Top 500 roles/titles
            "executive_names": executives[:1000],  # Top 1000 executive names
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
                "financial",
                "quarterly",
                "annual",
                "earnings",
                "revenue",
                "profit",
                "dividend",
                "stock",
                "shares",
                "market",
                "investment",
                "capital",
            ],
            "company_aliases": company_aliases[:1500],  # Top 1500 company aliases
            "company_names": companies[:500],  # Top 500 company names
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
                "The company reported",
                "Financial results show",
                "Market conditions indicate",
                "Regulatory compliance requires",
                "The board approved",
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
                {"name": "corporate_relevance", "enabled": True, "threshold": 0.6},
            ],
            "legal_filters": {
                "court_opinions_only": False,
                "include_dissents": True,
                "include_concurrences": True,
                "exclude_routine_matters": True,
            },
        },
        "performance": {
            "batch_size": 100,
            "max_workers": 4,
            "enable_caching": True,
            "cache_ttl": 3600,
        },
        "sp500_dow_integration": {
            "enabled": True,
            "executives_loaded": len(executives),
            "companies_loaded": len(companies),
            "company_aliases_loaded": len(company_aliases),
            "roles_loaded": len(roles),
            "config_version": "2.0",
            "last_updated": "2025-01-01",
            "sources": [
                "sp500_key_people_people.csv",
                "sp500_key_people_companies.csv",
                "sp500_key_people_appointments.csv",
                "sp500_key_people_roles.csv",
                "dow_key_people_people.csv",
                "dow_key_people_companies.csv",
                "dow_key_people_appointments.csv",
                "dow_key_people_roles.csv",
            ],
        },
    }

    return config


def main():
    """Generate and save comprehensive configuration."""
    # Extract all entities
    executives, companies, company_aliases, roles = extract_entities()

    # Create comprehensive configuration
    config = create_comprehensive_config(executives, companies, company_aliases, roles)

    # Save to enhanced config file
    output_path = Path("packages/corpus_extractors/configs/quotes_comprehensive.yaml")

    with open(output_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    print(f"\n✅ Comprehensive configuration saved to {output_path}")
    print("\nFinal Configuration Summary:")
    print("=" * 50)
    print(f"  Executive names: {config['sp500_dow_integration']['executives_loaded']}")
    print(f"  Company names: {config['sp500_dow_integration']['companies_loaded']}")
    print(
        f"  Company aliases: {config['sp500_dow_integration']['company_aliases_loaded']}"
    )
    print(f"  Role keywords: {config['sp500_dow_integration']['roles_loaded']}")
    print(
        f"  Total NLP recognition terms: {len(config['nlp']['role_keywords']) + len(config['nlp']['executive_names'])}"
    )
    print(
        f"  Total company recognition terms: {len(config['extraction']['company_aliases']) + len(config['extraction']['company_names'])}"
    )

    print("\nSample executives:")
    for name in executives[:10]:
        print(f"  - {name}")

    print("\nSample companies:")
    for company in companies[:10]:
        print(f"  - {company}")

    print("\nSample roles:")
    for role in roles[:10]:
        print(f"  - {role}")

    print(f"\nTo use this comprehensive configuration:")
    print("python -m corpus_extractors.cli.extract quotes \\")
    print("    --input data/courtlistener_normalized.jsonl \\")
    print("    --output data/quotes.jsonl \\")
    print("    --config configs/quotes_comprehensive.yaml")


if __name__ == "__main__":
    main()
