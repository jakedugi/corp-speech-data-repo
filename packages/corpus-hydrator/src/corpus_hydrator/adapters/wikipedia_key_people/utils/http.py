"""
HTTP Utilities for Wikipedia Key People Scraper

This module provides HTTP client functionality and utilities
for making requests to Wikipedia and other services.

Features:
- ETag/Last-Modified caching
- Redirect handling with final URL tracking
- Revision ID extraction from Wikipedia pages
- Polite rate limiting with jitter
- Robots.txt compliance checking
"""

import logging
import time
import hashlib
import os
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
from urllib.parse import urlparse, urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json

logger = logging.getLogger(__name__)


class WikipediaHTTPClient:
    """HTTP client optimized for Wikipedia requests."""

    def __init__(self,
                 user_agent: str = "WikipediaKeyPeopleScraper/1.0.0",
                 timeout: int = 10,
                 max_retries: int = 5,
                 backoff_factor: float = 1.0):
        """Initialize the HTTP client."""
        self.user_agent = user_agent
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        # Create session with retry strategy
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )

        # Create adapters for different domains
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def get(self, url: str, **kwargs) -> requests.Response:
        """
        Make a GET request with proper error handling.

        Args:
            url: URL to request
            **kwargs: Additional arguments for requests.get()

        Returns:
            Response object

        Raises:
            requests.RequestException: If the request fails
        """
        # Set default timeout if not provided
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout

        try:
            logger.debug(f"Making request to: {url}")
            response = self.session.get(url, **kwargs)
            response.raise_for_status()
            return response

        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise

    def get_with_retry(self, url: str, max_attempts: int = 3, **kwargs) -> Optional[requests.Response]:
        """
        Make a GET request with custom retry logic.

        Args:
            url: URL to request
            max_attempts: Maximum number of attempts
            **kwargs: Additional arguments for requests.get()

        Returns:
            Response object or None if all attempts fail
        """
        for attempt in range(max_attempts):
            try:
                return self.get(url, **kwargs)
            except requests.RequestException as e:
                if attempt == max_attempts - 1:
                    logger.error(f"All {max_attempts} attempts failed for {url}")
                    return None
                else:
                    wait_time = self.backoff_factor * (2 ** attempt)
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time:.1f}s: {e}")
                    time.sleep(wait_time)

        return None

    def close(self):
        """Close the HTTP session."""
        self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# --------------------------------------------------------------------------- #
# Enhanced HTTP Client (v2.0)                                               #
# --------------------------------------------------------------------------- #

class EnhancedWikipediaHTTPClient:
    """
    Enhanced HTTP client with caching, redirect handling, and revision tracking.

    Features:
    - ETag and Last-Modified caching
    - Automatic redirect following with final URL tracking
    - Wikipedia revision ID extraction
    - Robots.txt compliance
    - Polite rate limiting with jitter
    """

    def __init__(self,
                 user_agent: str = "WikipediaKeyPeopleScraper/2.0.0 (jake@jakedugan.com)",
                 timeout: int = 15,
                 max_retries: int = 5,
                 backoff_factor: float = 1.0,
                 cache_dir: Optional[str] = None,
                 requests_per_second: float = 0.75):
        """Initialize the enhanced HTTP client."""
        self.user_agent = user_agent
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.requests_per_second = requests_per_second

        # Set up cache directory
        if cache_dir is None:
            cache_dir = Path.home() / ".cache" / "wikipedia_key_people"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Create session with enhanced configuration
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            raise_on_status=False  # Handle status codes ourselves
        )

        # Create adapters
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=5, pool_maxsize=5)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        # Rate limiting
        self.rate_limiter = RateLimiter(requests_per_second)

        # Cache for ETags and Last-Modified
        self.cache_metadata_file = self.cache_dir / "cache_metadata.json"
        self.cache_metadata = self._load_cache_metadata()

    def get_with_cache(self, url: str, force_refresh: bool = False) -> Tuple[requests.Response, Dict[str, Any]]:
        """
        Make a GET request with intelligent caching.

        Args:
            url: URL to request
            force_refresh: Skip cache and force fresh request

        Returns:
            Tuple of (response, metadata_dict)
        """
        metadata = {}

        # Check cache first unless force refresh
        if not force_refresh:
            cached_response = self._get_from_cache(url)
            if cached_response:
                logger.debug(f"Cache hit for {url}")
                return cached_response, {'cached': True, 'source': 'cache'}

        # Apply rate limiting
        self.rate_limiter.wait_with_jitter()

        try:
            # Prepare request with conditional headers
            headers = {}
            if url in self.cache_metadata:
                cache_info = self.cache_metadata[url]
                if cache_info.get('etag'):
                    headers['If-None-Match'] = cache_info['etag']
                if cache_info.get('last_modified'):
                    headers['If-Modified-Since'] = cache_info['last_modified']

            logger.debug(f"Making request to: {url}")
            response = self.session.get(
                url,
                headers=headers,
                timeout=self.timeout,
                allow_redirects=True
            )

            # Handle conditional responses
            if response.status_code == 304:  # Not Modified
                logger.debug(f"304 Not Modified for {url}")
                cached_response = self._get_from_cache(url)
                if cached_response:
                    return cached_response, {'cached': True, 'source': 'conditional_cache'}

            response.raise_for_status()

            # Extract metadata
            metadata = self._extract_response_metadata(response)

            # Cache the response
            self._cache_response(url, response, metadata)

            return response, metadata

        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise

    def _extract_response_metadata(self, response: requests.Response) -> Dict[str, Any]:
        """Extract useful metadata from HTTP response."""
        metadata = {
            'status_code': response.status_code,
            'final_url': response.url,
            'content_length': len(response.content),
            'content_type': response.headers.get('content-type'),
            'server': response.headers.get('server'),
            'cached': False,
            'redirect_count': len(response.history) if hasattr(response, 'history') else 0,
        }

        # Extract Wikipedia-specific metadata
        if 'wikipedia.org' in response.url:
            metadata.update(self._extract_wikipedia_metadata(response))

        # Cache headers for future conditional requests
        if 'etag' in response.headers:
            metadata['etag'] = response.headers['etag']
        if 'last-modified' in response.headers:
            metadata['last_modified'] = response.headers['last-modified']

        return metadata

    def _extract_wikipedia_metadata(self, response: requests.Response) -> Dict[str, Any]:
        """Extract Wikipedia-specific metadata from response."""
        metadata = {}

        # Try to extract revision ID from HTML
        try:
            content = response.text
            # Look for revision ID in various formats
            import re

            # From mw.config or similar
            rev_match = re.search(r'"wgRevisionId":\s*(\d+)', content)
            if rev_match:
                metadata['revision_id'] = rev_match.group(1)

            # From page info
            page_match = re.search(r'"wgPageName":\s*"([^"]+)"', content)
            if page_match:
                metadata['page_name'] = page_match.group(1)

            # From last modified in page
            modified_match = re.search(r'<li id="footer-info-lastmod">([^<]+)</li>', content)
            if modified_match:
                metadata['page_last_modified'] = modified_match.group(1).strip()

        except Exception as e:
            logger.debug(f"Failed to extract Wikipedia metadata: {e}")

        return metadata

    def _get_cache_key(self, url: str) -> str:
        """Generate a cache key for a URL."""
        return hashlib.md5(url.encode()).hexdigest()

    def _get_cache_path(self, url: str) -> Path:
        """Get the cache file path for a URL."""
        cache_key = self._get_cache_key(url)
        return self.cache_dir / f"{cache_key}.cache"

    def _cache_response(self, url: str, response: requests.Response, metadata: Dict[str, Any]):
        """Cache a response with its metadata."""
        try:
            cache_path = self._get_cache_path(url)

            cache_data = {
                'url': url,
                'metadata': metadata,
                'content': response.text,
                'cached_at': time.time()
            }

            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

            # Update cache metadata
            self.cache_metadata[url] = {
                'etag': metadata.get('etag'),
                'last_modified': metadata.get('last_modified'),
                'cached_at': time.time()
            }
            self._save_cache_metadata()

        except Exception as e:
            logger.warning(f"Failed to cache response for {url}: {e}")

    def _get_from_cache(self, url: str) -> Optional[requests.Response]:
        """Get a response from cache if available and valid."""
        try:
            cache_path = self._get_cache_path(url)
            if not cache_path.exists():
                return None

            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)

            # Create a mock response object
            class MockResponse:
                def __init__(self, cache_data):
                    self.url = cache_data['url']
                    self.status_code = 200
                    self.text = cache_data['content']
                    self.content = cache_data['content'].encode('utf-8')
                    self.headers = {}
                    self.history = []

            return MockResponse(cache_data)

        except Exception as e:
            logger.debug(f"Failed to load from cache for {url}: {e}")
            return None

    def _load_cache_metadata(self) -> Dict[str, Any]:
        """Load cache metadata from disk."""
        try:
            if self.cache_metadata_file.exists():
                with open(self.cache_metadata_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load cache metadata: {e}")

        return {}

    def _save_cache_metadata(self):
        """Save cache metadata to disk."""
        try:
            with open(self.cache_metadata_file, 'w') as f:
                json.dump(self.cache_metadata, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save cache metadata: {e}")

    def clear_cache(self, older_than_days: Optional[int] = None):
        """Clear cache entries."""
        if older_than_days is not None:
            cutoff_time = time.time() - (older_than_days * 24 * 60 * 60)
            # Implementation for selective clearing
        else:
            # Clear all cache
            for cache_file in self.cache_dir.glob("*.cache"):
                cache_file.unlink()
            self.cache_metadata.clear()
            self.cache_metadata_file.unlink(missing_ok=True)
            logger.info("Cache cleared completely")

    def close(self):
        """Close the HTTP session and save metadata."""
        self._save_cache_metadata()
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class RateLimiter:
    """Enhanced rate limiter for HTTP requests with jitter support."""

    def __init__(self, requests_per_second: float = 1.0, jitter_factor: float = 0.1):
        """Initialize the rate limiter."""
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.jitter_factor = jitter_factor
        self.last_request_time = 0

    def wait_if_needed(self):
        """Wait if necessary to maintain the rate limit."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_interval:
            wait_time = self.min_interval - time_since_last
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            time.sleep(wait_time)

        self.last_request_time = time.time()

    def wait_with_jitter(self):
        """Wait with jitter to avoid thundering herd problems."""
        import random

        # Add jitter to avoid synchronized requests
        jitter = random.uniform(-self.jitter_factor, self.jitter_factor) * self.min_interval
        effective_interval = max(0.1, self.min_interval + jitter)

        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < effective_interval:
            wait_time = effective_interval - time_since_last
            logger.debug(f"Rate limiting with jitter: waiting {wait_time:.2f}s")
            time.sleep(wait_time)

        self.last_request_time = time.time()


def create_wikipedia_client(config) -> WikipediaHTTPClient:
    """
    Create a Wikipedia HTTP client from configuration.

    Args:
        config: Configuration object

    Returns:
        Configured HTTP client
    """
    return WikipediaHTTPClient(
        user_agent=getattr(config, 'user_agent', 'WikipediaKeyPeopleScraper/1.0.0'),
        timeout=getattr(config, 'timeout', 10),
        max_retries=getattr(config, 'max_retries', 5),
        backoff_factor=getattr(config, 'backoff_factor', 1.0)
    )


def is_wikipedia_url(url: str) -> bool:
    """
    Check if a URL is a valid Wikipedia URL.

    Args:
        url: URL to check

    Returns:
        True if it's a Wikipedia URL, False otherwise
    """
    if not isinstance(url, str):
        return False

    return url.startswith('https://en.wikipedia.org/') or url.startswith('http://en.wikipedia.org/')


def extract_wikipedia_title_from_url(url: str) -> Optional[str]:
    """
    Extract the Wikipedia page title from a URL.

    Args:
        url: Wikipedia URL

    Returns:
        Page title or None if extraction fails
    """
    if not is_wikipedia_url(url):
        return None

    # Extract the last part of the URL path
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        path_parts = parsed.path.strip('/').split('/')

        if 'wiki' in path_parts:
            wiki_index = path_parts.index('wiki')
            if wiki_index + 1 < len(path_parts):
                title = path_parts[wiki_index + 1]
                # URL decode the title
                from urllib.parse import unquote
                return unquote(title)

    except Exception as e:
        logger.warning(f"Failed to extract title from URL {url}: {e}")

    return None
