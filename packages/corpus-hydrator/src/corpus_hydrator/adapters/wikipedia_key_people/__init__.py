"""
Wikipedia Key People Adapter

A comprehensive adapter for extracting key people information from Wikipedia
market index pages and individual company pages.

Features:
- Extracts actual Wikipedia links from index tables (not constructed URLs)
- Scrapes key people from individual company pages with enhanced parsing
- Advanced data cleaning and normalization
- Clean architecture with proper separation of concerns
- SSOT (Single Source of Truth) data models
- Comprehensive test coverage
"""

from .config import WikipediaKeyPeopleScraperConfig, get_default_config
from .usecase import WikipediaKeyPeopleUseCase
from .writer import WikipediaKeyPeopleWriter
from .normalize import WikipediaKeyPeopleNormalizer
from .core.scraper import WikipediaKeyPeopleScraper, WikipediaLinkExtractor
from .core.enhanced_scraper import EnhancedWikipediaKeyPeopleExtractor

__version__ = "1.0.0"
__all__ = [
    # Main components
    "WikipediaKeyPeopleUseCase",
    "WikipediaKeyPeopleScraper",
    "WikipediaKeyPeopleWriter",
    "WikipediaKeyPeopleNormalizer",

    # Configuration
    "WikipediaKeyPeopleScraperConfig",
    "get_default_config",

    # Core components
    "WikipediaLinkExtractor",
    "EnhancedWikipediaKeyPeopleExtractor",
]
