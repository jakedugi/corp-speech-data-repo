"""
Configuration for Wikipedia Key People Scraper

This module provides configuration classes and default settings
for the Wikipedia key people extraction functionality.
"""

from dataclasses import dataclass
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


# Index Constituents Configuration (copied from extraction/config.py)

@dataclass
class IndexConfig:
    """Configuration for a specific market index."""

    name: str
    url: str
    table_id: str
    table_class: str
    columns: List[str]
    extract_columns: List[str]

    @property
    def table_selector(self) -> str:
        """CSS selector for finding the constituents table."""
        return f"table#{self.table_id}"


# Index-specific configurations
INDEX_CONFIGS = {
    "sp500": IndexConfig(
        name="S&P 500",
        url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        table_id="constituents",
        table_class="wikitable sortable",
        columns=[
            "Symbol",
            "Security",
            "GICS Sector",
            "GICS Sub-Industry",
            "Headquarters Location",
            "Date first added",
            "CIK",
            "Founded",
        ],
        extract_columns=["Symbol", "Security", "GICS Sector", "Date first added"],
    ),
    "dow": IndexConfig(
        name="Dow Jones Industrial Average",
        url="https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
        table_id="constituents",
        table_class="wikitable sortable",
        columns=[
            "Symbol",
            "Company",
            "Industry",
            "Date added",
            "Notes",
        ],
        extract_columns=["Symbol", "Company", "Industry", "Date added"],
    ),
    "nasdaq100": IndexConfig(
        name="Nasdaq 100",
        url="https://en.wikipedia.org/wiki/NASDAQ-100",
        table_id="constituents",
        table_class="wikitable sortable",
        columns=[
            "Company",
            "Ticker",
            "GICS Sector",
            "GICS Sub-Industry",
            "Founded",
        ],
        extract_columns=["Ticker", "Company", "GICS Sector", "GICS Sub-Industry"],
    ),
}


def get_index_config(index_name: str) -> IndexConfig:
    """Get configuration for a specific index."""
    if index_name not in INDEX_CONFIGS:
        available = list(INDEX_CONFIGS.keys())
        raise ValueError(f"Unknown index: {index_name}. Available: {available}")

    return INDEX_CONFIGS[index_name]


def get_available_indexes() -> List[str]:
    """Get list of available index names."""
    return list(INDEX_CONFIGS.keys())


def validate_index_name(index_name: str) -> bool:
    """Validate if an index name is supported."""
    return index_name in INDEX_CONFIGS


def normalize_index_name(index_name: str) -> str:
    """Normalize index name to standard format."""
    index_name = index_name.lower().strip()
    # Handle common variations
    if index_name in ["djia", "dow jones", "dow-jones"]:
        return "dow"
    elif index_name in ["s&p", "s&p500", "sp-500"]:
        return "sp500"
    elif index_name in ["nasdaq", "nasdaq-100", "nasdaq100"]:
        return "nasdaq100"
    return index_name
