"""
Base Provider for Wikipedia Key People

This module contains the base provider functionality for sourcing
company data and Wikipedia URLs.
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class BaseWikipediaProvider(ABC):
    """Base class for Wikipedia data providers."""

    def __init__(self, config):
        """Initialize the provider."""
        self.config = config

    @abstractmethod
    def get_companies_for_index(self, index_name: str) -> List[Dict[str, Any]]:
        """
        Get companies for a specific index.

        Args:
            index_name: Name of the index (e.g., 'sp500', 'dow')

        Returns:
            List of company dictionaries with ticker, name, wikipedia_url
        """
        pass

    @abstractmethod
    def get_provider_name(self) -> str:
        """Get the name of this provider."""
        pass

    def validate_company_data(self, company_data: Dict[str, Any]) -> bool:
        """
        Validate company data structure.

        Args:
            company_data: Company data dictionary

        Returns:
            True if valid, False otherwise
        """
        required_fields = ['ticker', 'company_name', 'wikipedia_url']

        for field in required_fields:
            if field not in company_data:
                logger.warning(f"Missing required field: {field}")
                return False

            if not company_data[field]:
                logger.warning(f"Empty required field: {field}")
                return False

        # Validate Wikipedia URL
        url = company_data['wikipedia_url']
        if not isinstance(url, str) or not url.startswith('https://en.wikipedia.org/'):
            logger.warning(f"Invalid Wikipedia URL: {url}")
            return False

        return True


# --------------------------------------------------------------------------- #
# People Data Providers (v2.0)                                              #
# --------------------------------------------------------------------------- #

from typing import Protocol, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ProviderPayload:
    """Payload returned by a people provider."""
    url: str
    content: Any  # HTML string, JSON dict, etc.
    content_type: str  # 'html', 'json', 'xml'
    revision_id: Optional[str] = None
    last_modified: Optional[str] = None
    etag: Optional[str] = None
    fetched_at: datetime = None

    def __post_init__(self):
        if self.fetched_at is None:
            self.fetched_at = datetime.now()


class PeopleProvider(Protocol):
    """Protocol for providers that can fetch people data."""

    def fetch_company_people(self, company: Dict[str, Any]) -> Optional[ProviderPayload]:
        """
        Fetch people data for a company.

        Args:
            company: Company dict with ticker, company_name, wikipedia_url, etc.

        Returns:
            ProviderPayload with structured people data, or None if not available
        """
        ...

    def get_provider_name(self) -> str:
        """Get the name of this provider."""
        ...

    def get_confidence_score(self) -> float:
        """Get the confidence score for this provider's data quality."""
        ...
