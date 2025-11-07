"""
HTTP Utilities with Caching and ETag Support

This module provides HTTP client utilities with intelligent caching,
ETag support, and retry logic for reliable data fetching.
"""

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..providers.base import ProviderError


class HttpCache:
    """Simple file-based HTTP cache with ETag support."""

    def __init__(self, cache_dir: Path, ttl_seconds: int = 6 * 3600):  # 6 hours default
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = ttl_seconds

    def _get_cache_key(self, url: str) -> str:
        """Generate cache key from URL."""
        return hashlib.md5(url.encode()).hexdigest()

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get cache file path."""
        return self.cache_dir / f"{cache_key}.json"

    def get(self, url: str) -> Optional[Dict[str, Any]]:
        """Get cached response if valid."""
        cache_key = self._get_cache_key(url)
        cache_path = self._get_cache_path(cache_key)

        if not cache_path.exists():
            return None

        try:
            with open(cache_path, "r") as f:
                cached = json.load(f)

            # Check TTL
            if time.time() - cached["timestamp"] > self.ttl_seconds:
                return None

            return cached

        except (json.JSONDecodeError, KeyError):
            return None

    def put(self, url: str, response: requests.Response, content: str) -> None:
        """Cache response with metadata."""
        cache_key = self._get_cache_key(url)
        cache_path = self._get_cache_path(cache_key)

        cached_data = {
            "url": url,
            "content": content,
            "timestamp": time.time(),
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
        }

        with open(cache_path, "w") as f:
            json.dump(cached_data, f, indent=2)


class HttpClient:
    """HTTP client with caching, ETags, and retry logic."""

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        user_agent: str = "IndexConstituents/1.0",
    ):
        self.cache = HttpCache(cache_dir) if cache_dir else None
        self.user_agent = user_agent

        # Configure session with retries
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            }
        )

        # Configure retries
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def get(self, url: str, force_refresh: bool = False) -> str:
        """
        Get URL content with caching and ETag support.

        Args:
            url: URL to fetch
            force_refresh: Bypass cache if True

        Returns:
            Response content as string

        Raises:
            ProviderError: If request fails
        """
        # Check cache first (unless force refresh)
        if not force_refresh and self.cache:
            cached = self.cache.get(url)
            if cached:
                # Check if we have ETag and can make conditional request
                etag = cached.get("etag")
                if etag:
                    headers = {"If-None-Match": etag}
                    response = self.session.get(url, headers=headers, timeout=10)
                    if response.status_code == 304:  # Not modified
                        return cached["content"]
                    elif response.status_code == 200:
                        # Content changed, update cache
                        self.cache.put(url, response, response.text)
                        return response.text
                else:
                    # No ETag, use cached content
                    return cached["content"]

        # Fetch fresh content
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            content = response.text

            # Cache if enabled
            if self.cache:
                self.cache.put(url, response, content)

            return content

        except requests.RequestException as e:
            raise ProviderError("http_client", urlparse(url).netloc, str(e)) from e

    def clear_cache(self) -> None:
        """Clear all cached responses."""
        if self.cache:
            for cache_file in self.cache.cache_dir.glob("*.json"):
                cache_file.unlink()

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        if not self.cache:
            return {"enabled": False}

        cache_files = list(self.cache.cache_dir.glob("*.json"))
        return {
            "enabled": True,
            "cache_dir": str(self.cache.cache_dir),
            "cached_urls": len(cache_files),
            "ttl_seconds": self.cache.ttl_seconds,
        }
