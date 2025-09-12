"""
Base Wikipedia People Extractor

This module contains the base extractor class that both the original and enhanced
extractors inherit from, avoiding circular import issues.
"""

import logging
from typing import List, Optional
from urllib.parse import urljoin
import re

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPerson

# Set up logging
logger = logging.getLogger(__name__)

# ────────── HTTP Setup ──────────
HEADERS = {"User-Agent": "jake@jakedugan.com"}

session = requests.Session()
session.headers.update(HEADERS)
retry_strategy = requests.packages.urllib3.util.retry.Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
session.mount("https://data.sec.gov", adapter)
session.mount("https://www.sec.gov", adapter)

wiki_adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
session.mount("https://en.wikipedia.org", wiki_adapter)

requests.packages.urllib3.disable_warnings(XMLParsedAsHTMLWarning)


class BaseWikipediaPeopleExtractor:
    """
    Base class for Wikipedia people extractors.

    Contains common functionality and utilities used by all extractors.
    """

    def __init__(self, config):
        """Initialize the base extractor."""
        self.config = config

    def _parse_person_text(self, text: str, company: dict) -> Optional[WikipediaKeyPerson]:
        """
        Parse person text into a WikipediaKeyPerson object.

        This is the core parsing logic used by all extractors.
        """
        if not text or not text.strip():
            return None

        text = text.strip()

        # Skip obviously malformed entries
        malformed_indicators = [
            text in ['(', ')', ',', '&', '[', ']', 'chairman', 'president', 'CEO', 'CFO', 'COO'],
            len(text) < 3,
            text.isdigit(),
            text in ['Executive', 'Director']
        ]

        if any(malformed_indicators):
            return None

        # Try different parsing patterns
        for pattern in self.config.content.name_title_patterns:
            match = re.match(pattern, text, re.IGNORECASE)
            if match:
                name_part = match.group(1).strip()
                title_part = match.group(2).strip() if len(match.groups()) > 1 else ""

                # Clean and normalize
                clean_name = self._clean_name(name_part)
                clean_title = self._clean_title(title_part or "Executive")

                if clean_name and clean_title:
                    return WikipediaKeyPerson(
                        ticker=company["ticker"],
                        company_name=company["company_name"],
                        raw_name=text,
                        clean_name=clean_name,
                        clean_title=clean_title,
                        wikipedia_url=company["wikipedia_url"],
                        extraction_method="pattern_matching",
                        parse_success=True,
                        confidence_score=0.9 if len(match.groups()) > 1 else 0.7
                    )

        # If no pattern matches but text looks like a name, assume it's just a name
        if 3 <= len(text) <= 100 and not any(char.isdigit() for char in text):
            clean_name = self._clean_name(text)
            if clean_name:
                return WikipediaKeyPerson(
                    ticker=company["ticker"],
                    company_name=company["company_name"],
                    raw_name=text,
                    clean_name=clean_name,
                    clean_title="Executive",
                    wikipedia_url=company["wikipedia_url"],
                    extraction_method="name_only",
                    parse_success=True,
                    confidence_score=0.5
                )

        return None

    def _clean_name(self, name: str) -> str:
        """Clean up executive name."""
        if not name:
            return ""

        # Remove extra whitespace
        name = ' '.join(name.split())

        # Remove common prefixes/suffixes that aren't part of the name
        name = re.sub(r'^(Mr\.|Mrs\.|Ms\.|Dr\.)\s+', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*(Jr\.|Sr\.|III|II|IV)$', '', name, flags=re.IGNORECASE)

        return name.strip()

    def _clean_title(self, title: str) -> str:
        """Clean up executive title."""
        if not title:
            return ""

        # Remove extra whitespace
        title = ' '.join(title.split())

        # Apply title normalization patterns
        for pattern, replacement in self.config.content.title_normalization.items():
            title = re.sub(pattern, replacement, title, flags=re.IGNORECASE)

        return title.strip()

    def _extract_people_from_section(self, section_element, company: dict) -> List[WikipediaKeyPerson]:
        """Extract individual people from a section element."""
        people = []

        # Try list items first (most common format)
        list_items = section_element.find_all("li")
        if list_items:
            for li in list_items:
                text = li.get_text(" ", strip=True)
                person = self._parse_person_text(text, company)
                if person:
                    people.append(person)
        else:
            # Fallback to pipe-separated text
            text = section_element.get_text(separator="|")
            items = [item.strip() for item in text.split("|") if item.strip()]

            for item in items:
                person = self._parse_person_text(item, company)
                if person:
                    people.append(person)

        return people
