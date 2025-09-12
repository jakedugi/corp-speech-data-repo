"""
Enums and Constants for Wikipedia Key People Scraper

This module defines enumerations and constants used throughout the scraper
for better type safety and maintainability.
"""

from enum import Enum, auto
from typing import Set


class IndexType(Enum):
    """Supported market indices."""
    DOW = "dow"
    SP500 = "sp500"
    NASDAQ100 = "nasdaq100"

    @classmethod
    def from_string(cls, value: str) -> "IndexType":
        """Create IndexType from string, case-insensitive."""
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        raise ValueError(f"Unsupported index type: {value}")

    @property
    def display_name(self) -> str:
        """Get human-readable display name."""
        return {
            self.DOW: "Dow Jones Industrial Average",
            self.SP500: "S&P 500",
            self.NASDAQ100: "NASDAQ-100"
        }[self]


class ExtractionMethod(Enum):
    """Methods used to extract people data."""
    INFOBOX = "infobox"
    SECTION = "section"
    TABLE = "table"
    LIST = "list"
    WIKIDATA = "wikidata"

    @classmethod
    def get_confidence_score(cls, method: "ExtractionMethod") -> float:
        """Get confidence score for extraction method."""
        return {
            cls.WIKIDATA: 0.95,
            cls.INFOBOX: 0.90,
            cls.SECTION: 0.80,
            cls.TABLE: 0.70,
            cls.LIST: 0.60
        }.get(method, 0.50)


class ProviderPriority(Enum):
    """Provider priority order for data extraction."""
    WIKIDATA = auto()
    WIKIPEDIA_INFOBOX = auto()
    WIKIPEDIA_SECTION = auto()
    WIKIPEDIA_TABLE = auto()
    WIKIPEDIA_LIST = auto()

    @property
    def weight(self) -> int:
        """Get priority weight (higher = better)."""
        return {
            self.WIKIDATA: 100,
            self.WIKIPEDIA_INFOBOX: 90,
            self.WIKIPEDIA_SECTION: 80,
            self.WIKIPEDIA_TABLE: 70,
            self.WIKIPEDIA_LIST: 60
        }[self]


class DataQuality(Enum):
    """Data quality levels."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"

    @classmethod
    def from_confidence(cls, confidence: float) -> "DataQuality":
        """Determine quality from confidence score."""
        if confidence >= 0.9:
            return cls.HIGH
        elif confidence >= 0.7:
            return cls.MEDIUM
        elif confidence >= 0.5:
            return cls.LOW
        else:
            return cls.UNKNOWN


class ContentType(Enum):
    """HTTP content types."""
    HTML = "text/html"
    JSON = "application/json"
    XML = "application/xml"
    TEXT = "text/plain"

    @classmethod
    def from_mimetype(cls, mimetype: str) -> "ContentType":
        """Create ContentType from MIME type string."""
        mimetype = mimetype.lower()
        for content_type in cls:
            if content_type.value in mimetype:
                return content_type
        return cls.TEXT


# ============================================================================
# Controlled Vocabularies
# ============================================================================

# These are moved from normalize.py for better organization

ROLE_VOCABULARY: Set[str] = {
    # C-Suite
    "CEO", "CHIEF_EXECUTIVE_OFFICER",
    "CFO", "CHIEF_FINANCIAL_OFFICER",
    "COO", "CHIEF_OPERATING_OFFICER",
    "CTO", "CHIEF_TECHNOLOGY_OFFICER",
    "CIO", "CHIEF_INFORMATION_OFFICER",
    "CMO", "CHIEF_MARKETING_OFFICER",
    "CHRO", "CHIEF_HUMAN_RESOURCES_OFFICER",
    "CSO", "CHIEF_STRATEGY_OFFICER",
    "CLO", "CHIEF_LEGAL_OFFICER",
    "CAO", "CHIEF_ADMINISTRATIVE_OFFICER",
    "CBO", "CHIEF_BUSINESS_OFFICER",

    # Leadership
    "CHAIR", "CHAIRMAN", "CHAIRWOMAN", "CHAIRPERSON",
    "VICE_CHAIR", "VICE_CHAIRMAN", "VICE_CHAIRWOMAN",
    "PRESIDENT", "VICE_PRESIDENT", "EXECUTIVE_PRESIDENT",

    # Board
    "BOARD_MEMBER", "BOARD_CHAIR", "BOARD_VICE_CHAIR",
    "DIRECTOR", "NON_EXECUTIVE_DIRECTOR",

    # Operations
    "GENERAL_COUNSEL", "SECRETARY", "TREASURER",
    "SENIOR_VICE_PRESIDENT", "VICE_PRESIDENT",

    # Founders and Special Roles
    "FOUNDER", "CO_FOUNDER", "EXECUTIVE_CHAIRMAN",
    "NON_EXECUTIVE_CHAIRMAN", "LEAD_DIRECTOR"
}

# Common non-person terms to filter out
NON_PERSON_TERMS: Set[str] = {
    "the company", "founders", "leadership team", "management team",
    "executive team", "board of directors", "our team", "senior management",
    "key personnel", "corporate officers", "company officials"
}

# Wikipedia-specific URL patterns
WIKIPEDIA_URL_PATTERNS = {
    'base': 'https://en.wikipedia.org/wiki/',
    'api': 'https://en.wikipedia.org/w/api.php',
    'index_dow': 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average',
    'index_sp500': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
    'index_nasdaq': 'https://en.wikipedia.org/wiki/NASDAQ-100'
}

# Wikidata API patterns
WIKIDATA_URL_PATTERNS = {
    'api': 'https://www.wikidata.org/w/api.php',
    'sparql': 'https://query.wikidata.org/sparql',
    'entity': 'https://www.wikidata.org/wiki/'
}


class URLValidator:
    """URL validation utilities."""

    @staticmethod
    def is_valid_wikipedia_url(url: str) -> bool:
        """Check if URL is a valid Wikipedia URL."""
        if not isinstance(url, str):
            return False

        return (
            url.startswith('https://en.wikipedia.org/wiki/') or
            url.startswith('http://en.wikipedia.org/wiki/')
        ) and len(url) > len('https://en.wikipedia.org/wiki/')

    @staticmethod
    def is_valid_wikidata_url(url: str) -> bool:
        """Check if URL is a valid Wikidata URL."""
        if not isinstance(url, str):
            return False

        return (
            url.startswith('https://www.wikidata.org/wiki/') or
            url.startswith('http://www.wikidata.org/wiki/')
        )

    @staticmethod
    def extract_wikipedia_title(url: str) -> str:
        """Extract page title from Wikipedia URL."""
        if not URLValidator.is_valid_wikipedia_url(url):
            raise ValueError(f"Invalid Wikipedia URL: {url}")

        # Remove base URL and decode
        title_part = url.split('/wiki/')[-1]
        from urllib.parse import unquote
        return unquote(title_part)

    @staticmethod
    def extract_wikidata_qid(url: str) -> str:
        """Extract QID from Wikidata URL."""
        if not URLValidator.is_valid_wikidata_url(url):
            raise ValueError(f"Invalid Wikidata URL: {url}")

        # Extract QID (e.g., Q95 from https://www.wikidata.org/wiki/Q95)
        qid_part = url.split('/wiki/')[-1]
        if not qid_part.startswith('Q') or not qid_part[1:].isdigit():
            raise ValueError(f"Invalid Wikidata QID in URL: {url}")

        return qid_part

    @staticmethod
    def build_wikipedia_url(title: str) -> str:
        """Build Wikipedia URL from page title."""
        from urllib.parse import quote
        encoded_title = quote(title.replace(' ', '_'))
        return f"{WIKIPEDIA_URL_PATTERNS['base']}{encoded_title}"

    @staticmethod
    def build_wikidata_url(qid: str) -> str:
        """Build Wikidata URL from QID."""
        if not (qid.startswith('Q') and qid[1:].isdigit()):
            raise ValueError(f"Invalid Wikidata QID: {qid}")
        return f"{WIKIDATA_URL_PATTERNS['entity']}{qid}"
