"""
Financial Modeling Prep (FMP) Provider - PLACEHOLDER

This is a placeholder implementation for FMP API integration.
FMP provides financial market data including index constituents.

TODO: Implement full FMP API integration for:
- S&P 500 constituents via /api/v3/sp500_constituent
- Historical changes via /api/v3/historical/sp500_constituent
- Other indexes if available

Note: FMP may require paid subscription for full access.
"""

import logging
from typing import Mapping, Any

from .base import IndexProvider, ProviderError

logger = logging.getLogger(__name__)


class FMPProvider(IndexProvider):
    """
    FMP-based provider for index constituents.

    PLACEHOLDER: This provider is not yet implemented.
    Requires FMP API key and may need paid subscription.
    """

    def __init__(self, api_key: str = None):
        """
        Initialize FMP provider.

        Args:
            api_key: FMP API key (required for production use)
        """
        self.api_key = api_key
        if not api_key:
            logger.warning("FMP provider initialized without API key")

    @property
    def name(self) -> str:
        """Provider name for logging."""
        return "fmp"

    @property
    def priority(self) -> int:
        """Priority for fallback ordering (2 = medium priority)."""
        return 2

    def fetch_raw(self, index_key: str) -> Mapping[str, Any]:
        """
        Fetch raw data from FMP API.

        PLACEHOLDER: Not yet implemented.

        Args:
            index_key: The index identifier

        Returns:
            Dictionary containing raw JSON data

        Raises:
            ProviderError: Always raises error (not implemented)
        """
        raise ProviderError(
            self.name,
            index_key,
            "FMP provider is not yet implemented. "
            "Requires API key and potential paid subscription. "
            "Use Wikipedia provider instead."
        )
