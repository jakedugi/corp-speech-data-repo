"""
Yahoo ETF Provider - PLACEHOLDER

This is a placeholder implementation for Yahoo Finance ETF data.
Yahoo provides ETF holdings data that can be used as index constituents.

TODO: Implement Yahoo Finance ETF integration for:
- SPY (S&P 500 ETF) holdings
- DIA (Dow Jones ETF) holdings
- QQQ (Nasdaq 100 ETF) holdings

Note: May require scraping or unofficial API access.
"""

import logging
from typing import Any, Mapping

from .base import IndexProvider, ProviderError

logger = logging.getLogger(__name__)


class YahooETFProvider(IndexProvider):
    """
    Yahoo Finance ETF-based provider for index constituents.

    PLACEHOLDER: This provider is not yet implemented.
    Uses ETF holdings as proxy for index constituents.
    """

    def __init__(self, cache_dir=None):
        """
        Initialize Yahoo ETF provider.

        Args:
            cache_dir: Directory for caching ETF data
        """
        self.cache_dir = cache_dir
        logger.info("Yahoo ETF provider initialized (placeholder)")

    @property
    def name(self) -> str:
        """Provider name for logging."""
        return "yahoo_etf"

    @property
    def priority(self) -> int:
        """Priority for fallback ordering (3 = lower priority)."""
        return 3

    def fetch_raw(self, index_key: str) -> Mapping[str, Any]:
        """
        Fetch ETF holdings data from Yahoo Finance.

        PLACEHOLDER: Not yet implemented.

        Args:
            index_key: The index identifier

        Returns:
            Dictionary containing ETF holdings data

        Raises:
            ProviderError: Always raises error (not implemented)
        """
        # Map index to ETF ticker
        etf_map = {"sp500": "SPY", "dow": "DIA", "nasdaq100": "QQQ"}

        etf_ticker = etf_map.get(index_key)
        if not etf_ticker:
            raise ProviderError(
                self.name, index_key, f"No ETF mapping available for index: {index_key}"
            )

        raise ProviderError(
            self.name,
            index_key,
            f"Yahoo ETF provider is not yet implemented. "
            f"Would fetch holdings for {etf_ticker} ETF. "
            f"Use Wikipedia provider instead.",
        )
