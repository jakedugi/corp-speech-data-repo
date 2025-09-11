"""
Scraper configuration and data models.

This module defines the authoritative schemas for controlling Wikipedia scraper behavior,
including index-specific configurations, rate limiting, and data validation.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Literal, Any
from pydantic import BaseModel, Field, validator
import re


# --------------------------------------------------------------------------- #
# Index Types and Configurations                                              #
# --------------------------------------------------------------------------- #


class IndexConfig(BaseModel):
    """Configuration for a specific market index."""

    name: str = Field(..., description="Index name (e.g., 'S&P 500', 'Dow Jones')")
    short_name: str = Field(..., description="Short identifier (e.g., 'sp500', 'dow')")
    wikipedia_url: str = Field(..., description="Wikipedia list URL")
    table_id: str = Field(default="constituents", description="HTML table ID to parse")
    ticker_column: int = Field(default=0, description="Column index for ticker symbols")
    name_column: int = Field(default=1, description="Column index for company names")
    cik_column: Optional[int] = Field(default=6, description="Column index for CIK numbers")
    max_companies: Optional[int] = Field(None, description="Limit number of companies to scrape")

    @validator("wikipedia_url")
    def validate_wikipedia_url(cls, v: str) -> str:
        """Ensure URL is a valid Wikipedia URL."""
        if not v.startswith("https://en.wikipedia.org/"):
            raise ValueError("URL must be a valid Wikipedia URL")
        return v


class ScrapingConfig(BaseModel):
    """Configuration for scraping behavior."""

    # Rate limiting (matching original scraper)
    wikipedia_rate_limit: float = Field(default=1.0, description="Requests per second for Wikipedia")
    sec_rate_limit: float = Field(default=10.0, description="Requests per second for SEC EDGAR (token bucket max 10 per second)")

    # Data limits
    max_people_per_company: int = Field(default=100, description="Maximum people to extract per company")
    max_people_per_title: int = Field(default=5, description="Maximum people per title type")
    max_companies: Optional[int] = Field(None, description="Global limit on companies to process")

    # Retry and timeout settings
    request_timeout: int = Field(default=10, description="HTTP request timeout in seconds")
    max_retries: int = Field(default=5, description="Maximum retry attempts")
    backoff_factor: float = Field(default=1.0, description="Exponential backoff factor")

    # HTTP pool settings
    wikipedia_pool_connections: int = Field(default=50, description="Wikipedia connection pool size")
    wikipedia_pool_maxsize: int = Field(default=50, description="Wikipedia max pool size")
    sec_pool_connections: int = Field(default=10, description="SEC connection pool size")
    sec_pool_maxsize: int = Field(default=10, description="SEC max pool size")

    # Output settings
    output_dir: str = Field(default="data", description="Output directory for scraped data")
    wide_format_filename: str = Field(default="{index}_aliases_enriched.csv", description="Wide format output filename")
    long_format_filename: str = Field(default="{index}_officers_cleaned.csv", description="Long format output filename")
    base_list_filename: str = Field(default="{index}_aliases.csv", description="Base company list filename")


class ContentExtractionConfig(BaseModel):
    """Configuration for content extraction patterns."""

    # Wikipedia infobox keywords (matching original scraper)
    role_keywords: List[str] = Field(
        default_factory=lambda: [
            "people", "key people", "leadership", "management",
            "executive", "governing", "board", "director",
            "officer", "team"
        ],
        description="Keywords to search for in Wikipedia infobox headers (matches original scraper)"
    )

    # SEC filing patterns
    sec_filing_type: str = Field(default="DEF 14A", description="SEC filing type to scrape")
    officer_table_patterns: List[str] = Field(
        default_factory=lambda: [
            "Name and Principal Occupation",
            "Executive Officers",
            "Directors and Executive Officers"
        ],
        description="Patterns to identify officer tables in SEC filings"
    )

    # Title filtering patterns (exactly matching original scraper SPX_TITLES)
    officer_title_patterns: Dict[str, str] = Field(
        default_factory=lambda: {
            # C-Suite (exactly as in original)
            "chief_executive_officer": r"\\bchief executive officer\\b",
            "ceo": r"\\bceo\\b",
            "chief_financial_officer": r"\\bchief financial officer\\b",
            "cfo": r"\\bcfo\\b",
            "chief_operating_officer": r"\\bchief operating officer\\b",
            "coo": r"\\bcoo\\b",
            "chief_legal_officer": r"\\bchief legal officer\\b",
            "clo": r"\\bclo\\b",
            "general_counsel": r"\\bgeneral counsel\\b",
            "treasurer": r"\\btreasurer\\b",
            # Board leadership (exactly as in original)
            "chairman": r"\\bchair(man|person)?\\b",
            "board_director": r"\\bboard director\\b",
            "director": r"\\bdirector\\b",
        },
        description="Regex patterns for officer title filtering (exactly matches original SPX_TITLES)"
    )

    # Fallback text extraction patterns (matching original scraper)
    fallback_patterns: List[str] = Field(
        default_factory=lambda: [
            r"[•\-\*]?\s*([A-Z][a-zA-Z\s\.'-]+)\s+[\u2014\-]+\s+([A-Za-z ,]+)",
        ],
        description="Regex patterns for fallback text extraction (matches original fallback_text_search)"
    )

    # SEC heading regex (exactly matching original HEADING_REGEX)
    sec_heading_regex: str = Field(
        default=r"""(?xi)
        (
          executive\s+officers
          | directors\s+and\s+executive\s+officers
          | board\s+of\s+directors
          | key\s+people
          | leadership\s+team
          | management\s+team
          | corporate\s+officers?
          | section\s+16\s+officers?
          | named\s+executive\s+officers?
          | [""]?officers[""]?
        )""",
        description="Regex pattern for SEC filing headings (exactly matches original HEADING_REGEX)"
    )


class ValidationConfig(BaseModel):
    """Configuration for data validation."""

    # CIK validation
    require_cik: bool = Field(default=True, description="Require CIK for all companies")
    cik_pattern: str = Field(default=r"^\d{10}$", description="CIK validation regex pattern")

    # Company name validation
    require_company_name: bool = Field(default=True, description="Require company name")
    min_name_length: int = Field(default=2, description="Minimum company name length")

    # Ticker validation
    require_ticker: bool = Field(default=True, description="Require ticker symbol")
    ticker_pattern: str = Field(default=r"^[A-Z]{1,5}$", description="Ticker validation regex pattern")

    # Data quality checks
    min_officers_required: int = Field(default=1, description="Minimum officers required per company")
    max_officers_per_company: int = Field(default=50, description="Maximum officers per company")
    require_unique_officers: bool = Field(default=True, description="Require unique officer names per company")


# --------------------------------------------------------------------------- #
# Main Scraper Configuration                                                   #
# --------------------------------------------------------------------------- #


class WikipediaScraperConfig(BaseModel):
    """Complete configuration for Wikipedia scraper."""

    version: str = Field(default="1.0.0", description="Configuration version")

    # Index configuration
    indices: Dict[str, IndexConfig] = Field(
        default_factory=lambda: {
            "sp500": IndexConfig(
                name="S&P 500",
                short_name="sp500",
                wikipedia_url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                table_id="constituents",
                ticker_column=0,
                name_column=1,
                cik_column=6,
                max_companies=None,  # No limit by default
            ),
            "dow": IndexConfig(
                name="Dow Jones Industrial Average",
                short_name="dow",
                wikipedia_url="https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
                table_id="constituents",
                ticker_column=0,  # Symbol column in mock table
                name_column=1,   # Company column in mock table
                cik_column=None, # No CIK column in mock table
            ),
            "nasdaq100": IndexConfig(
                name="NASDAQ-100",
                short_name="nasdaq100",
                wikipedia_url="https://en.wikipedia.org/wiki/NASDAQ-100",
                table_id="constituents",
                ticker_column=0,
                name_column=1,
                cik_column=3,
            ),
            "russell1000": IndexConfig(
                name="Russell 1000",
                short_name="russell1000",
                wikipedia_url="https://en.wikipedia.org/wiki/Russell_1000_Index",
                table_id="constituents",
                ticker_column=0,
                name_column=1,
                cik_column=None,  # Russell 1000 may not have CIKs
            ),
        },
        description="Available market indices configurations"
    )

    # Scraping behavior
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig)

    # Content extraction
    extraction: ContentExtractionConfig = Field(default_factory=ContentExtractionConfig)

    # Validation
    validation: ValidationConfig = Field(default_factory=ValidationConfig)

    # Runtime settings
    enabled_indices: List[str] = Field(
        default_factory=lambda: ["sp500"],
        description="Which indices to scrape"
    )
    dry_run: bool = Field(default=False, description="Run without making HTTP requests")
    verbose: bool = Field(default=False, description="Enable verbose logging")

    # User agent and headers
    user_agent: str = Field(
        default="WikipediaScraper/1.0.0 (contact@example.com)",
        description="HTTP User-Agent header"
    )

    def get_index_config(self, index_name: str) -> IndexConfig:
        """Get configuration for a specific index."""
        if index_name not in self.indices:
            raise ValueError(f"Unknown index: {index_name}")
        return self.indices[index_name]

    def get_active_indices(self) -> List[IndexConfig]:
        """Get configurations for all enabled indices."""
        return [self.indices[name] for name in self.enabled_indices if name in self.indices]


# --------------------------------------------------------------------------- #
# Data Models                                                                  #
# --------------------------------------------------------------------------- #


class CompanyRecord(BaseModel):
    """Data model for a company record."""

    ticker: str = Field(..., description="Stock ticker symbol")
    official_name: str = Field(..., description="Full company name")
    cik: Optional[str] = Field(None, description="SEC Central Index Key")
    wikipedia_url: str = Field(..., description="Wikipedia page URL")
    index_name: str = Field(..., description="Market index this company belongs to")

    @validator("ticker")
    def validate_ticker(cls, v: str) -> str:
        """Validate ticker format."""
        if not re.match(r"^[A-Z]{1,5}(\.[A-Z])?$", v):
            raise ValueError("Invalid ticker format")
        return v.upper()

    @validator("cik")
    def validate_cik(cls, v: Optional[str]) -> Optional[str]:
        """Validate CIK format if provided."""
        if v is not None and not re.match(r"^\d{10}$", v):
            raise ValueError("CIK must be 10 digits")
        return v


class OfficerRecord(BaseModel):
    """Data model for an executive officer record."""

    name: str = Field(..., description="Officer full name")
    title: str = Field(..., description="Officer title/role")
    company_ticker: str = Field(..., description="Company ticker")
    company_name: str = Field(..., description="Company name")
    cik: Optional[str] = Field(None, description="Company CIK")
    source: Literal["wikipedia", "sec_edgar"] = Field(..., description="Data source")
    scraped_at: datetime = Field(default_factory=datetime.now, description="When this data was scraped")

    @validator("name")
    def validate_name(cls, v: str) -> str:
        """Validate officer name."""
        if not v or not v.strip():
            raise ValueError("Officer name cannot be empty")
        return v.strip()

    @validator("title")
    def validate_title(cls, v: str) -> str:
        """Validate officer title."""
        if not v or not v.strip():
            raise ValueError("Officer title cannot be empty")
        return v.strip()


class ScrapingResult(BaseModel):
    """Result model for scraping operations."""

    index_name: str = Field(..., description="Index that was scraped")
    companies_found: int = Field(default=0, description="Number of companies found")
    companies_processed: int = Field(default=0, description="Number of companies successfully processed")
    officers_found: int = Field(default=0, description="Total officers found")
    errors: List[str] = Field(default_factory=list, description="Error messages")
    started_at: datetime = Field(default_factory=datetime.now, description="When scraping started")
    completed_at: Optional[datetime] = Field(None, description="When scraping completed")
    duration_seconds: Optional[float] = Field(None, description="Total duration in seconds")

    def mark_completed(self):
        """Mark the scraping as completed."""
        self.completed_at = datetime.now()
        if self.started_at and self.completed_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()


# --------------------------------------------------------------------------- #
# Default Configurations                                                      #
# --------------------------------------------------------------------------- #


def get_default_config() -> WikipediaScraperConfig:
    """Get default scraper configuration."""
    return WikipediaScraperConfig()


def get_sp500_config() -> WikipediaScraperConfig:
    """Get configuration optimized for S&P 500 scraping."""
    config = get_default_config()
    config.enabled_indices = ["sp500"]
    return config


def get_multi_index_config() -> WikipediaScraperConfig:
    """Get configuration for scraping multiple indices."""
    config = get_default_config()
    config.enabled_indices = ["sp500", "dow", "nasdaq100"]
    config.scraping.max_companies = 100  # Limit for testing
    return config


# --------------------------------------------------------------------------- #
# Configuration Validation                                                    #
# --------------------------------------------------------------------------- #


def validate_config(config: WikipediaScraperConfig) -> List[str]:
    """Validate a scraper configuration and return any issues."""
    issues = []

    # Check enabled indices exist
    for index_name in config.enabled_indices:
        if index_name not in config.indices:
            issues.append(f"Enabled index '{index_name}' not found in indices configuration")

    # Check rate limits are reasonable
    if config.scraping.wikipedia_rate_limit > 10:
        issues.append("Wikipedia rate limit too high (>10 req/sec)")
    if config.scraping.sec_rate_limit > 20:
        issues.append("SEC rate limit too high (>20 req/sec)")

    # Check data limits make sense
    if config.validation.max_officers_per_company < config.validation.min_officers_required:
        issues.append("Max officers per company cannot be less than min officers required")

    return issues
