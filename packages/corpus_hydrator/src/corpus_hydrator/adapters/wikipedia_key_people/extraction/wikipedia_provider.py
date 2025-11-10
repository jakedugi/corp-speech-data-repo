"""
Wikipedia Index Provider

This module provides Wikipedia-based data fetching for index constituents.
It implements the IndexProvider protocol for fetching HTML content from Wikipedia.
"""

import logging
from pathlib import Path
from typing import Any, Mapping

from corpus_hydrator.adapters.wikipedia_key_people.config import get_index_config
from corpus_hydrator.adapters.wikipedia_key_people.utils.http import HttpClient
from .providers.base import IndexProvider, ProviderError

logger = logging.getLogger(__name__)


class WikipediaProvider(IndexProvider):
    """
    Wikipedia-based provider for index constituents.

    Fetches HTML content from Wikipedia pages containing index constituent lists.
    Supports intelligent caching and ETag-based conditional requests.
    """

    def __init__(
        self,
        cache_dir: Path = None,
        user_agent: str = "IndexConstituents/1.0 (educational@example.com)",
        force_refresh: bool = False,
    ):
        """
        Initialize Wikipedia provider.

        Args:
            cache_dir: Directory for HTTP cache (optional)
            user_agent: HTTP User-Agent header
            force_refresh: Bypass cache if True
        """
        self.http_client = HttpClient(cache_dir=cache_dir, user_agent=user_agent)
        self.force_refresh = force_refresh

    @property
    def name(self) -> str:
        """Provider name for logging."""
        return "wikipedia"

    @property
    def priority(self) -> int:
        """Priority for fallback ordering (1 = highest priority)."""
        return 1

    def fetch_raw(self, index_key: str) -> Mapping[str, Any]:
        """
        Fetch raw HTML content for the specified index.

        Args:
            index_key: The index identifier (e.g., 'sp500', 'dow', 'nasdaq100')

        Returns:
            Dictionary containing raw HTML and metadata

        Raises:
            ProviderError: If fetching fails
        """
        try:
            config = get_index_config(index_key)

            logger.info(f"Fetching {config.name} from Wikipedia: {config.url}")

            # Fetch HTML content with caching
            html_content = self.http_client.get(
                config.url, force_refresh=self.force_refresh
            )

            return {
                "content": html_content,
                "url": config.url,
                "index_key": index_key,
                "index_name": config.name,
                "source": "wikipedia",
                "format": "html",
            }

        except Exception as e:
            logger.error(f"Wikipedia provider failed for {index_key}: {e}")
            raise ProviderError(self.name, index_key, str(e)) from e

    def get_cache_stats(self) -> Mapping[str, Any]:
        """Get HTTP cache statistics."""
        return self.http_client.get_cache_stats()

    def clear_cache(self) -> None:
        """Clear HTTP cache."""
        self.http_client.clear_cache()
