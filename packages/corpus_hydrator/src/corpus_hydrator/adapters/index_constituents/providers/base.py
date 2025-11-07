"""
Base Index Provider Interface

This module defines the protocol/interface for index data providers.
Providers are responsible for fetching raw data from external sources.
"""

from abc import abstractmethod
from typing import Any, Mapping, Protocol


class IndexProvider(Protocol):
    """
    Protocol for index data providers.

    Providers are responsible for fetching raw data from external sources
    (Wikipedia, FMP, Yahoo ETF, etc.) and returning it in a normalized format.
    """

    @abstractmethod
    def fetch_raw(self, index_key: str) -> Mapping[str, Any]:
        """
        Fetch raw data for the specified index.

        Args:
            index_key: The index identifier (e.g., 'sp500', 'dow', 'nasdaq100')

        Returns:
            Mapping containing raw data (HTML string, JSON, etc.)

        Raises:
            ProviderError: If the provider fails to fetch data
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name for logging and identification."""
        pass

    @property
    @abstractmethod
    def priority(self) -> int:
        """Priority for fallback ordering (lower = higher priority)."""
        pass


class ProviderError(Exception):
    """Base exception for provider-related errors."""

    def __init__(self, provider_name: str, index_key: str, message: str):
        self.provider_name = provider_name
        self.index_key = index_key
        self.message = message
        super().__init__(f"{provider_name} failed for {index_key}: {message}")
