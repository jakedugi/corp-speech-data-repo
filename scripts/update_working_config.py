#!/usr/bin/env python3
"""
Update working_config.py with all unique role keywords, executive names, and company aliases
from S&P 500 and Dow Jones datasets.
"""

import csv
import re
from pathlib import Path


def clean_name(name):
    """Clean executive names by removing parentheses, titles, and extra whitespace."""
    if not name or len(name.strip()) < 3:
        return None

    # Remove common titles and parentheses
    name = re.sub(r"\s*\([^)]*\)\s*", "", name)
    name = re.sub(r"\s*\([^)]*$", "", name)  # Handle unclosed parentheses

    # Remove common role indicators
    name = re.sub(r"\s*\([^)]*\)\s*", "", name)
    name = re.sub(r"\s*\([^)]*$", "", name)

    # Clean up extra whitespace
    name = re.sub(r"\s+", " ", name).strip()

    # Skip very short names or common words
    if len(name) < 3 or name.lower() in ["the", "and", "inc", "ltd", "corp", "co"]:
        return None

    return name


def clean_role(role):
    """Clean role keywords."""
    if not role or len(role.strip()) < 2:
        return None

    role = role.strip()
    # Convert to title case for consistency
    role = role.title()

    return role


def clean_company_alias(alias):
    """Clean company aliases."""
    if not alias or len(alias.strip()) < 2:
        return None

    alias = alias.strip()
    # Skip very short aliases
    if len(alias) < 2:
        return None

    return alias.lower()


def main():
    # Data structures to collect unique values
    executive_names = set()
    role_keywords = set()
    company_aliases = set()

    # Files to process
    data_dir = Path("data")

    # Process S&P 500 and Dow Jones people files for executive names
    people_files = ["sp500_key_people_people.csv", "dow_key_people_people.csv"]

    for filename in people_files:
        filepath = data_dir / filename
        if filepath.exists():
            print(f"Processing {filename}...")
            with open(filepath, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = clean_name(row.get("normalized_name", ""))
                    if name:
                        executive_names.add(name)

    # Process roles files for role keywords
    roles_files = ["sp500_key_people_roles.csv", "dow_key_people_roles.csv"]

    for filename in roles_files:
        filepath = data_dir / filename
        if filepath.exists():
            print(f"Processing {filename}...")
            with open(filepath, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Add both canonical and raw role names
                    canon_role = clean_role(row.get("role_canon", ""))
                    raw_role = clean_role(row.get("role_raw", ""))
                    if canon_role:
                        role_keywords.add(canon_role)
                    if raw_role:
                        role_keywords.add(raw_role)

    # Process company files for company aliases
    company_files = ["sp500_key_people_companies.csv", "dow_key_people_companies.csv"]

    for filename in company_files:
        filepath = data_dir / filename
        if filepath.exists():
            print(f"Processing {filename}...")
            with open(filepath, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Add company name
                    company_name = clean_company_alias(row.get("company_name", ""))
                    if company_name:
                        company_aliases.add(company_name)

                    # Add ticker
                    ticker = clean_company_alias(row.get("ticker", ""))
                    if ticker:
                        company_aliases.add(ticker)

    # Convert sets to sorted lists
    executive_names_list = sorted(list(executive_names))
    role_keywords_list = sorted(list(role_keywords))
    company_aliases_list = sorted(list(company_aliases))

    print(f"Found {len(executive_names_list)} unique executive names")
    print(f"Found {len(role_keywords_list)} unique role keywords")
    print(f"Found {len(company_aliases_list)} unique company aliases")

    # Read current working_config.py
    config_path = Path("working_config.py")
    with open(config_path, "r") as f:
        current_content = f.read()

    # Update the config values
    # Update executive_names
    exec_pattern = r'("executive_names": \[)[^\]]*(\])'
    current_exec_match = re.search(exec_pattern, current_content, re.DOTALL)
    if current_exec_match:
        exec_list_str = ",\n        ".join(f'"{name}"' for name in executive_names_list)
        new_exec_section = f'"executive_names": [\n        {exec_list_str}\n    ]'
        current_content = re.sub(
            exec_pattern, new_exec_section, current_content, flags=re.DOTALL
        )

    # Update role_keywords
    role_pattern = r'("role_keywords": \[)[^\]]*(\])'
    current_role_match = re.search(role_pattern, current_content, re.DOTALL)
    if current_role_match:
        role_list_str = ",\n        ".join(f'"{role}"' for role in role_keywords_list)
        new_role_section = f'"role_keywords": [\n        {role_list_str}\n    ]'
        current_content = re.sub(
            role_pattern, new_role_section, current_content, flags=re.DOTALL
        )

    # Update company_aliases
    alias_pattern = r'("company_aliases": \[)[^\]]*(\])'
    current_alias_match = re.search(alias_pattern, current_content, re.DOTALL)
    if current_alias_match:
        alias_list_str = ",\n        ".join(
            f'"{alias}"' for alias in company_aliases_list
        )
        new_alias_section = f'"company_aliases": [\n        {alias_list_str}\n    ]'
        current_content = re.sub(
            alias_pattern, new_alias_section, current_content, flags=re.DOTALL
        )

    # Write updated config
    with open(config_path, "w") as f:
        f.write(current_content)

    print(f"Updated working_config.py with comprehensive data:")
    print(f"  - {len(executive_names_list)} executive names")
    print(f"  - {len(role_keywords_list)} role keywords")
    print(f"  - {len(company_aliases_list)} company aliases")


if __name__ == "__main__":
    main()
