"""
Base Parser for Wikipedia Key People

This module contains the base parsing functionality for extracting
key people data from Wikipedia pages.
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPerson

logger = logging.getLogger(__name__)


class BaseWikipediaParser(ABC):
    """Base class for Wikipedia page parsers."""

    def __init__(self, config):
        """Initialize the parser."""
        self.config = config

    @abstractmethod
    def parse_page(self, html_content: str, company_data: dict) -> List[WikipediaKeyPerson]:
        """
        Parse a Wikipedia page for key people.

        Args:
            html_content: Raw HTML content of the page
            company_data: Dictionary with company information

        Returns:
            List of extracted key people
        """
        pass

    @abstractmethod
    def get_parser_name(self) -> str:
        """Get the name of this parser."""
        pass


class WikipediaPageSection:
    """Represents a section of a Wikipedia page."""

    def __init__(self, heading: str, content_element, level: int = 2):
        """Initialize a page section."""
        self.heading = heading
        self.content_element = content_element
        self.level = level

    def contains_people_keywords(self, keywords: List[str]) -> bool:
        """Check if this section likely contains people information."""
        heading_lower = self.heading.lower()

        for keyword in keywords:
            if keyword in heading_lower:
                return True

        return False

    def extract_text_content(self) -> str:
        """Extract text content from this section."""
        if self.content_element:
            return self.content_element.get_text(" ", strip=True)
        return ""

    def find_tables(self):
        """Find all tables in this section."""
        if self.content_element:
            return self.content_element.find_all("table")
        return []

    def find_lists(self):
        """Find all lists in this section."""
        if self.content_element:
            return self.content_element.find_all(["ul", "ol"])
        return []
