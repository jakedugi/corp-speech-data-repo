#!/usr/bin/env python3
"""
Fix the corrupted working_config.py by recreating it cleanly.
"""

import csv
from pathlib import Path
import re

def clean_name_strict(name):
    """Aggressively clean names to only include letters, spaces, hyphens, and apostrophes."""
    if not name or len(name.strip()) < 3:
        return None

    # Remove parentheses and their contents
    name = re.sub(r'\s*\([^)]*\)\s*', '', name)
    name = re.sub(r'\s*\([^)]*$', '', name)

    # Only allow letters, spaces, hyphens, apostrophes, and periods
    name = re.sub(r'[^a-zA-Z\s\-\'\.]', '', name)

    # Clean up extra whitespace
    name = re.sub(r'\s+', ' ', name).strip()

    # Remove leading/trailing hyphens, apostrophes, periods
    name = re.sub(r'^[-\'\.]+|[-\'\.]+$', '', name)

    # Skip if too short, contains isolated letters, or only special chars
    if len(name) < 3 or re.match(r'^[a-zA-Z]$', name) or not re.search(r'[a-zA-Z]{2,}', name):
        return None

    # Skip entries that look like they're just fragments or codes
    if re.match(r'^\s*[a-zA-Z]\s*$', name):  # Single letter
        return None

    return name

def clean_role_canon(role):
    """Clean role canonical names - should be standardized."""
    if not role or len(role.strip()) < 2:
        return None

    role = role.strip().upper()  # Canonical roles are usually uppercase

    # Only allow letters and underscores (common in canonical names)
    if not re.match(r'^[A-Z_]+$', role):
        return None

    # Skip very short roles
    if len(role) < 2:
        return None

    return role

def clean_company_alias(alias):
    """Clean company names and tickers."""
    if not alias or len(alias.strip()) < 2:
        return None

    alias = alias.strip()

    # For tickers (usually 1-5 uppercase letters), allow only letters
    if re.match(r'^[A-Z]{1,5}$', alias):
        return alias.lower()

    # For company names, allow letters, spaces, hyphens, apostrophes, periods, ampersands
    alias = re.sub(r'[^a-zA-Z\s\-\'\.&]', '', alias)
    alias = re.sub(r'\s+', ' ', alias).strip()

    # Remove leading/trailing punctuation
    alias = re.sub(r'^[-\'\.&]+|[-\'\.&]+$', '', alias)

    if len(alias) < 3 or not re.search(r'[a-zA-Z]{2,}', alias):
        return None

    return alias.lower()

def main():
    # Data structures to collect unique values
    executive_names = set()
    role_keywords = set()
    company_aliases = set()

    # Files to process
    data_dir = Path('data')

    # Process S&P 500 and Dow Jones people files for executive names (only normalized_name)
    people_files = [
        'sp500_key_people_people.csv',
        'dow_key_people_people.csv'
    ]

    for filename in people_files:
        filepath = data_dir / filename
        if filepath.exists():
            with open(filepath, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = clean_name_strict(row.get('normalized_name', ''))
                    if name:
                        executive_names.add(name)

    # Process roles files for role keywords (only role_canon)
    roles_files = [
        'sp500_key_people_roles.csv',
        'dow_key_people_roles.csv'
    ]

    for filename in roles_files:
        filepath = data_dir / filename
        if filepath.exists():
            with open(filepath, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    role = clean_role_canon(row.get('role_canon', ''))
                    if role:
                        role_keywords.add(role)

    # Process company files for company aliases
    company_files = [
        'sp500_key_people_companies.csv',
        'dow_key_people_companies.csv'
    ]

    for filename in company_files:
        filepath = data_dir / filename
        if filepath.exists():
            with open(filepath, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Add company name
                    company_name = clean_company_alias(row.get('company_name', ''))
                    if company_name:
                        company_aliases.add(company_name)

                    # Add ticker
                    ticker = clean_company_alias(row.get('ticker', ''))
                    if ticker:
                        company_aliases.add(ticker)

    # Convert sets to sorted lists
    executive_names_list = sorted(list(executive_names))
    role_keywords_list = sorted(list(role_keywords))
    company_aliases_list = sorted(list(company_aliases))

    # Create the clean config
    config_content = f'''# Working configuration for quote extraction with clean, normalized S&P 500 and Dow Jones data
# Only includes normalized columns with no numbers or special characters

config = {{
    "nlp": {{
        "spacy_model": "en_core_web_sm",
        "enable_ner": True,
        "use_gpu": False,
        "role_keywords": [
{chr(10).join(f'        "{role}"' for role in role_keywords_list)}
    ],
        "executive_names": [
{chr(10).join(f'        "{name}"' for name in executive_names_list)}
    ]
    }},
    "extraction": {{
        "company_aliases": [
{chr(10).join(f'        "{alias}"' for alias in company_aliases_list)}
    ],
        "keywords": ["regulation", "policy", "statement", "privacy", "data"]
    }},
    "reranking": {{
        "enabled": True,
        "model": "all-mpnet-base-v2",
        "threshold": 0.1,
        "seed_quotes": [
            "The company stated that",
            "According to the CEO",
            "We are committed to"
        ]
    }}
}}
'''

    # Write the clean config
    config_path = Path('working_config.py')
    with open(config_path, 'w') as f:
        f.write(config_content)

    print(f"✅ Created clean working_config.py with:")
    print(f"   - {len(executive_names_list)} executive names (normalized, no special chars)")
    print(f"   - {len(role_keywords_list)} role keywords (only canonical)")
    print(f"   - {len(company_aliases_list)} company aliases (cleaned)")

    # Show some samples
    print(f"\n📋 Sample executive names: {executive_names_list[:3]}")
    print(f"📋 Sample role keywords: {role_keywords_list[:3]}")
    print(f"📋 Sample company aliases: {company_aliases_list[:3]}")

if __name__ == '__main__':
    main()
