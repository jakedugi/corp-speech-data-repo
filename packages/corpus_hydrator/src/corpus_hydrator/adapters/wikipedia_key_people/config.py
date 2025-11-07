"""
Configuration for Wikipedia Key People Scraper

This module provides configuration classes and default settings
for the Wikipedia key people extraction functionality.
"""

from typing import Dict, List, Optional

from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPeopleConfig
from pydantic import BaseModel, Field


class WikipediaKeyPeopleScraperConfig(BaseModel):
    """Main configuration for the Wikipedia key people scraper."""

    # Core scraper configuration
    scraper: WikipediaKeyPeopleConfig = Field(default_factory=WikipediaKeyPeopleConfig)

    # Output configuration
    output_dir: str = Field(
        default="data", description="Output directory for scraped data"
    )
    save_intermediate: bool = Field(
        default=False, description="Save intermediate results"
    )

    # Performance configuration
    max_workers: int = Field(default=2, description="Maximum concurrent workers")
    batch_size: int = Field(default=10, description="Batch size for processing")

    # Logging configuration
    log_level: str = Field(default="INFO", description="Logging level")
    verbose: bool = Field(default=False, description="Enable verbose logging")


def get_default_config() -> WikipediaKeyPeopleScraperConfig:
    """Get default scraper configuration."""
    return WikipediaKeyPeopleScraperConfig()


def get_development_config() -> WikipediaKeyPeopleScraperConfig:
    """Get configuration optimized for development."""
    config = get_default_config()
    config.scraper.scraping.max_companies = 5  # Limit for testing
    config.scraper.scraping.max_people_per_company = 25
    config.verbose = True
    config.log_level = "DEBUG"
    return config


def get_production_config() -> WikipediaKeyPeopleScraperConfig:
    """Get configuration optimized for production."""
    config = get_default_config()
    config.scraper.scraping.max_companies = None  # No limit
    config.max_workers = 4  # More workers for production
    config.batch_size = 20
    return config
