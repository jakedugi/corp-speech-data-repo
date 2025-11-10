"""
Single Source of Truth (SSOT) Schema for Wikipedia Key People Data

This module defines the authoritative data models for Wikipedia key people extraction,
following clean architecture principles and providing a unified interface for all
Wikipedia scraping operations.

Version: 2.0.0
- Added normalized table structure (companies, people, roles, appointments)
- Added Wikidata support
- Enhanced normalization rules
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, validator

# Schema version for governance
__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Core Data Models                                                             #
# --------------------------------------------------------------------------- #


class WikipediaKeyPerson(BaseModel):
    """Core data model for a key person extracted from Wikipedia."""

    # Identity fields
    ticker: str = Field(..., description="Company stock ticker symbol")
    company_name: str = Field(..., description="Full company name")

    # Person information
    raw_name: str = Field(..., description="Original raw name from Wikipedia")
    clean_name: str = Field(..., description="Cleaned and normalized person name")
    clean_title: str = Field(..., description="Cleaned and standardized job title")

    # Metadata
    source: str = Field(default="wikipedia", description="Data source identifier")
    wikipedia_url: str = Field(
        ..., description="Wikipedia page URL where data was found"
    )
    extraction_method: str = Field(
        ..., description="Method used for extraction (infobox, section, etc.)"
    )
    scraped_at: datetime = Field(
        default_factory=datetime.now, description="When this data was scraped"
    )

    # Quality indicators
    parse_success: bool = Field(
        default=True, description="Whether parsing was successful"
    )
    confidence_score: float = Field(
        default=1.0, description="Confidence in data accuracy (0-1)"
    )

    @validator("ticker")
    def validate_ticker(cls, v: str) -> str:
        """Validate ticker format."""
        if not re.match(r"^[A-Z]{1,5}(\.[A-Z])?$", v.upper()):
            raise ValueError(
                "Ticker must be 1-5 uppercase letters optionally followed by . and letter"
            )
        return v.upper()

    @validator("clean_name")
    def validate_name(cls, v: str) -> str:
        """Validate and clean person name."""
        if not v or not v.strip():
            raise ValueError("Person name cannot be empty")

        # Clean up extra whitespace
        cleaned = " ".join(v.split())

        # Basic validation
        if len(cleaned) < 2:
            raise ValueError("Person name must be at least 2 characters")
        if len(cleaned) > 100:
            raise ValueError("Person name must be less than 100 characters")

        return cleaned

    @validator("clean_title")
    def validate_title(cls, v: str) -> str:
        """Validate and clean job title."""
        if not v or not v.strip():
            raise ValueError("Job title cannot be empty")

        # Clean up extra whitespace
        cleaned = " ".join(v.split())

        # Basic validation
        if len(cleaned) < 2:
            raise ValueError("Job title must be at least 2 characters")
        if len(cleaned) > 100:
            raise ValueError("Job title must be less than 100 characters")

        return cleaned


class WikipediaCompany(BaseModel):
    """Data model for a company processed from Wikipedia index pages."""

    ticker: str = Field(..., description="Company stock ticker symbol")
    company_name: str = Field(..., description="Full company name")
    wikipedia_url: str = Field(..., description="Actual Wikipedia company page URL")
    index_name: str = Field(
        ..., description="Index this company belongs to (sp500, dow, etc.)"
    )

    # Business metadata from index
    sector: Optional[str] = Field(None, description="Company GICS sector")
    industry: Optional[str] = Field(None, description="Company GICS industry/sub-industry")
    date_added: Optional[str] = Field(None, description="Date company was added to index")
    extracted_at: datetime = Field(
        default_factory=datetime.now, description="When index data was extracted"
    )
    source_url: str = Field(
        ..., description="Wikipedia index page URL where data was sourced"
    )

    # Processing metadata
    processed_at: datetime = Field(
        default_factory=datetime.now, description="When this company was processed"
    )
    key_people_count: int = Field(default=0, description="Number of key people found")
    processing_success: bool = Field(
        default=True, description="Whether processing was successful"
    )

    @validator("wikipedia_url")
    def validate_wikipedia_url(cls, v: str) -> str:
        """Validate Wikipedia URL format."""
        if not v.startswith("https://en.wikipedia.org/"):
            raise ValueError("URL must be a valid Wikipedia URL")
        return v


class WikipediaExtractionResult(BaseModel):
    """Result model for a complete Wikipedia extraction operation."""

    # Operation metadata
    operation_id: str = Field(
        ..., description="Unique identifier for this extraction operation"
    )
    index_name: str = Field(..., description="Index that was processed")
    started_at: datetime = Field(
        default_factory=datetime.now, description="When extraction started"
    )
    completed_at: Optional[datetime] = Field(
        None, description="When extraction completed"
    )

    # Results
    companies_processed: int = Field(
        default=0, description="Number of companies processed"
    )
    companies_successful: int = Field(
        default=0, description="Number of companies successfully processed"
    )
    total_key_people: int = Field(default=0, description="Total key people extracted")
    success_rate: float = Field(default=0.0, description="Success rate (0-1)")

    # Data
    companies: List[WikipediaCompany] = Field(
        default_factory=list, description="Processed companies"
    )
    key_people: List[WikipediaKeyPerson] = Field(
        default_factory=list, description="Extracted key people"
    )

    # Status
    success: bool = Field(
        default=True, description="Whether the operation was successful"
    )
    error_message: Optional[str] = Field(
        None, description="Error message if operation failed"
    )

    def mark_completed(self):
        """Mark the extraction as completed."""
        self.completed_at = datetime.now()
        if self.companies_processed > 0:
            self.success_rate = self.companies_successful / self.companies_processed


# --------------------------------------------------------------------------- #
# Configuration Models                                                        #
# --------------------------------------------------------------------------- #


class WikipediaScrapingConfig(BaseModel):
    """Configuration for Wikipedia scraping behavior."""

    # Rate limiting
    wikipedia_rate_limit: float = Field(
        default=1.0, description="Requests per second for Wikipedia"
    )
    request_timeout: int = Field(
        default=10, description="HTTP request timeout in seconds"
    )
    max_retries: int = Field(default=5, description="Maximum retry attempts")
    backoff_factor: float = Field(default=1.0, description="Exponential backoff factor")

    # Data limits
    max_people_per_company: int = Field(
        default=100, description="Maximum people to extract per company"
    )
    max_companies: Optional[int] = Field(
        None, description="Global limit on companies to process"
    )

    # HTTP pool settings
    pool_connections: int = Field(default=50, description="HTTP connection pool size")
    pool_maxsize: int = Field(default=50, description="HTTP max pool size")

    # Output settings
    output_dir: str = Field(
        default="data", description="Output directory for scraped data"
    )


class WikipediaContentConfig(BaseModel):
    """Configuration for content extraction patterns."""

    # Keywords to search for in Wikipedia infoboxes
    role_keywords: List[str] = Field(
        default_factory=lambda: [
            "people",
            "key people",
            "leadership",
            "management",
            "executive",
            "governing",
            "board",
            "director",
            "officer",
            "team",
            "executives",
            "board members",
            "directors",
            "corporate officers",
            "leadership team",
            "management team",
            "senior management",
            "chairman",
            "chairperson",
            "president",
            "vice president",
            "chief executive",
            "chief operating",
            "chief financial",
            "chief technology",
            "chief information",
            "chief legal",
            "chief marketing",
            "chief human resources",
            "chief strategy",
            "founder",
            "co-founder",
            "founders",
            "owners",
            "principal",
            "principals",
            "partners",
            "managing director",
            "senior vice president",
            "executive vice president",
        ],
        description="Keywords to search for in Wikipedia infobox headers",
    )

    # Title normalization patterns
    title_normalization: Dict[str, str] = Field(
        default_factory=lambda: {
            r"\bCEO\b": "Chief Executive Officer",
            r"\bCFO\b": "Chief Financial Officer",
            r"\bCOO\b": "Chief Operating Officer",
            r"\bCTO\b": "Chief Technology Officer",
            r"\bCIO\b": "Chief Information Officer",
            r"\bCMO\b": "Chief Marketing Officer",
            r"\bCHRO\b": "Chief Human Resources Officer",
            r"\bCSO\b": "Chief Strategy Officer",
            r"\bCLO\b": "Chief Legal Officer",
        },
        description="Regex patterns for title normalization",
    )

    # Parsing patterns for extracting names and titles
    name_title_patterns: List[str] = Field(
        default_factory=lambda: [
            r"^([^(]+?)\s*\(([^)]+)\)$",  # Name (Title)
            r"^([^—]+?)—\s*(.+)$",  # Name — Title
            r"^([^,]+?),\s*(.+)$",  # Name, Title
            r"^([^\[]+?)\s*\[.*\]$",  # Name [citation]
        ],
        description="Regex patterns for parsing names and titles",
    )


class WikipediaIndexConfig(BaseModel):
    """Configuration for a specific market index."""

    name: str = Field(..., description="Index name (e.g., 'S&P 500', 'Dow Jones')")
    short_name: str = Field(..., description="Short identifier (e.g., 'sp500', 'dow')")
    wikipedia_url: str = Field(..., description="Wikipedia list URL")
    table_id: str = Field(default="constituents", description="HTML table ID to parse")
    ticker_column: int = Field(default=0, description="Column index for ticker symbols")
    name_column: int = Field(default=1, description="Column index for company names")
    max_companies: Optional[int] = Field(
        None, description="Limit number of companies to scrape"
    )

    @validator("wikipedia_url")
    def validate_wikipedia_url(cls, v: str) -> str:
        """Ensure URL is a valid Wikipedia URL."""
        if not v.startswith("https://en.wikipedia.org/"):
            raise ValueError("URL must be a valid Wikipedia URL")
        return v


class WikipediaKeyPeopleConfig(BaseModel):
    """Complete configuration for Wikipedia key people extraction."""

    version: str = Field(default="1.0.0", description="Configuration version")

    # Index configurations
    indices: Dict[str, WikipediaIndexConfig] = Field(
        default_factory=lambda: {
            "sp500": WikipediaIndexConfig(
                name="S&P 500",
                short_name="sp500",
                wikipedia_url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                table_id="constituents",
                ticker_column=0,
                name_column=1,
                max_companies=None,
            ),
            "dow": WikipediaIndexConfig(
                name="Dow Jones Industrial Average",
                short_name="dow",
                wikipedia_url="https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
                table_id="constituents",
                ticker_column=1,  # Symbol column (2nd td element in row)
                name_column=0,  # Company column (1st th element in row)
                max_companies=None,
            ),
            "nasdaq100": WikipediaIndexConfig(
                name="Nasdaq 100",
                short_name="nasdaq100",
                wikipedia_url="https://en.wikipedia.org/wiki/NASDAQ-100",
                table_id="constituents",
                ticker_column=0,
                name_column=1,
                max_companies=None,
            ),
        },
        description="Available market indices configurations",
    )

    # Scraping configuration
    scraping: WikipediaScrapingConfig = Field(default_factory=WikipediaScrapingConfig)

    # Content extraction configuration
    content: WikipediaContentConfig = Field(default_factory=WikipediaContentConfig)

    # Runtime settings
    enabled_indices: List[str] = Field(
        default_factory=lambda: ["sp500"], description="Which indices to scrape"
    )
    dry_run: bool = Field(default=False, description="Run without making HTTP requests")
    verbose: bool = Field(default=False, description="Enable verbose logging")

    # User agent
    user_agent: str = Field(
        default="WikipediaKeyPeopleScraper/1.0.0 (contact@example.com)",
        description="HTTP User-Agent header",
    )

    def get_index_config(self, index_name: str) -> WikipediaIndexConfig:
        """Get configuration for a specific index."""
        if index_name not in self.indices:
            raise ValueError(f"Unknown index: {index_name}")
        return self.indices[index_name]

    def get_active_indices(self) -> List[WikipediaIndexConfig]:
        """Get configurations for all enabled indices."""
        return [
            self.indices[name] for name in self.enabled_indices if name in self.indices
        ]


# --------------------------------------------------------------------------- #
# Default Configurations                                                      #
# --------------------------------------------------------------------------- #


def get_default_config() -> WikipediaKeyPeopleConfig:
    """Get default scraper configuration."""
    return WikipediaKeyPeopleConfig()


def get_sp500_config() -> WikipediaKeyPeopleConfig:
    """Get configuration optimized for S&P 500 scraping."""
    config = get_default_config()
    config.enabled_indices = ["sp500"]
    return config


def get_multi_index_config() -> WikipediaKeyPeopleConfig:
    """Get configuration for scraping multiple indices."""
    config = get_default_config()
    config.enabled_indices = ["sp500", "dow", "nasdaq100"]
    config.scraping.max_companies = 100  # Limit for testing
    return config


# --------------------------------------------------------------------------- #
# Validation Functions                                                        #
# --------------------------------------------------------------------------- #


def validate_config(config: WikipediaKeyPeopleConfig) -> List[str]:
    """Validate a scraper configuration and return any issues."""
    issues = []

    # Check enabled indices exist
    for index_name in config.enabled_indices:
        if index_name not in config.indices:
            issues.append(
                f"Enabled index '{index_name}' not found in indices configuration"
            )

    # Check rate limits are reasonable
    if config.scraping.wikipedia_rate_limit > 10:
        issues.append("Wikipedia rate limit too high (>10 req/sec)")

    # Check data limits make sense
    if config.scraping.max_people_per_company < 1:
        issues.append("Max people per company must be at least 1")

    return issues


def validate_key_person(person: WikipediaKeyPerson) -> List[str]:
    """Validate a key person record and return any issues."""
    issues = []

    # Check required fields
    if not person.ticker:
        issues.append("Ticker is required")
    if not person.company_name:
        issues.append("Company name is required")
    if not person.clean_name:
        issues.append("Clean name is required")
    if not person.clean_title:
        issues.append("Clean title is required")

    # Check data quality
    if len(person.clean_name.split()) < 2 and not any(
        title in person.clean_title.lower()
        for title in ["chairman", "president", "founder"]
    ):
        issues.append("Name should typically have first and last name")

    if person.confidence_score < 0 or person.confidence_score > 1:
        issues.append("Confidence score must be between 0 and 1")

    return issues


# --------------------------------------------------------------------------- #
# Normalized Table Structure (Production Schema v2.0)                       #
# --------------------------------------------------------------------------- #


class NormalizedCompany(BaseModel):
    """Normalized company record for production database."""

    company_id: str = Field(
        ..., description="Unique stable identifier (ticker + index)"
    )
    company_name: str = Field(..., description="Full company name")
    ticker: str = Field(..., description="Stock ticker symbol")
    wikipedia_url: str = Field(..., description="Wikipedia company page URL")
    wikidata_qid: Optional[str] = Field(
        None, description="Wikidata QID (Q followed by digits)"
    )
    index_name: str = Field(..., description="Index this company belongs to")

    # Business metadata from index
    sector: Optional[str] = Field(None, description="Company GICS sector")
    industry: Optional[str] = Field(None, description="Company GICS industry/sub-industry")
    date_added: Optional[str] = Field(None, description="Date company was added to index")
    extracted_at: datetime = Field(
        default_factory=datetime.now, description="When index data was extracted"
    )
    source_url: str = Field(
        ..., description="Wikipedia index page URL where data was sourced"
    )

    processed_at: datetime = Field(
        default_factory=datetime.now, description="When this record was processed"
    )
    source_revision_id: Optional[str] = Field(None, description="Wikipedia revision ID")

    @validator("wikidata_qid")
    def validate_wikidata_qid(cls, v: Optional[str]) -> Optional[str]:
        """Validate Wikidata QID format."""
        if v and not re.match(r"^Q\d+$", v):
            raise ValueError("Wikidata QID must be in format Q12345")
        return v


class NormalizedPerson(BaseModel):
    """Normalized person record for production database."""

    person_id: str = Field(..., description="Unique stable identifier")
    full_name: str = Field(..., description="Original full name as extracted")
    normalized_name: str = Field(..., description="Normalized, cleaned name")
    wikidata_qid: Optional[str] = Field(
        None, description="Wikidata QID for this person"
    )
    created_at: datetime = Field(
        default_factory=datetime.now, description="When this record was created"
    )

    @validator("normalized_name")
    def validate_normalized_name(cls, v: str) -> str:
        """Ensure normalized name is not empty and properly formatted."""
        if not v or not v.strip():
            raise ValueError("Normalized name cannot be empty")
        return v.strip()


# Controlled vocabulary for roles - defined as module-level constant
NORMALIZED_ROLE_VOCABULARY = {
    "CEO",
    "CFO",
    "COO",
    "CTO",
    "CIO",
    "CMO",
    "CRO",
    "CCO",
    "CSO",
    "CGO",
    "CHAIR",
    "VICE_CHAIR",
    "PRESIDENT",
    "VICE_PRESIDENT",
    "FOUNDER",
    "CO_FOUNDER",
    "EXECUTIVE",
    "BOARD_MEMBER",
    "BOARD_CHAIR",
    "BOARD_VICE_CHAIR",
    "EXECUTIVE_CHAIRMAN",
    "NON_EXECUTIVE_CHAIRMAN",
    "SENIOR_VICE_PRESIDENT",
    "VICE_PRESIDENT",
    "GENERAL_COUNSEL",
    "SECRETARY",
    "TREASURER",
    "CHIEF_EXECUTIVE_OFFICER",
    "CHIEF_FINANCIAL_OFFICER",
    "CHIEF_OPERATING_OFFICER",
    "CHIEF_TECHNOLOGY_OFFICER",
    "CHIEF_INFORMATION_OFFICER",
    "CHIEF_MARKETING_OFFICER",
    "CHIEF_RISK_OFFICER",
    "CHIEF_COMPLIANCE_OFFICER",
    "CHIEF_STRATEGY_OFFICER",
    "CHIEF_GROWTH_OFFICER",
}


class NormalizedRole(BaseModel):
    """Normalized role record with controlled vocabulary."""

    role_id: str = Field(..., description="Unique stable identifier")
    role_canon: str = Field(
        ..., description="Canonical role from controlled vocabulary"
    )
    role_raw: str = Field(..., description="Original raw role text")

    @validator("role_canon")
    def validate_role_canon(cls, v: str) -> str:
        """Ensure role is in controlled vocabulary."""
        if v.upper() not in NORMALIZED_ROLE_VOCABULARY:
            raise ValueError(f"Role '{v}' not in controlled vocabulary")
        return v.upper()


class NormalizedAppointment(BaseModel):
    """Normalized appointment linking companies, people, and roles."""

    company_id: str = Field(..., description="Reference to company")
    person_id: str = Field(..., description="Reference to person")
    role_id: str = Field(..., description="Reference to role")
    start_date: Optional[datetime] = Field(None, description="Appointment start date")
    end_date: Optional[datetime] = Field(None, description="Appointment end date")
    source_url: str = Field(..., description="Source URL where this was found")
    source_revision_id: Optional[str] = Field(None, description="Source revision ID")
    extraction_strategy: str = Field(..., description="Strategy used for extraction")
    confidence_score: float = Field(
        default=1.0, ge=0.0, le=1.0, description="Confidence in this appointment"
    )
    extracted_at: datetime = Field(
        default_factory=datetime.now, description="When this was extracted"
    )

    @validator("end_date")
    def validate_date_range(
        cls, v: Optional[datetime], values: Dict[str, Any]
    ) -> Optional[datetime]:
        """Ensure end_date is after start_date."""
        if v and values.get("start_date") and v < values["start_date"]:
            raise ValueError("End date must be after start date")
        return v


# --------------------------------------------------------------------------- #
# Production Dataset Manifest                                               #
# --------------------------------------------------------------------------- #


class DatasetManifest(BaseModel):
    """Manifest for a production dataset with governance metadata."""

    schema_version: str = Field(..., description="Schema version (SemVer)")
    dataset_name: str = Field(..., description="Dataset identifier")
    extracted_at: datetime = Field(
        default_factory=datetime.now, description="When extraction completed"
    )
    source: str = Field(..., description="Data source (wikipedia, wikidata, etc.)")
    provider_order: List[str] = Field(..., description="Provider priority order used")

    # File metadata
    companies_count: int = Field(default=0, description="Number of companies")
    people_count: int = Field(default=0, description="Number of people")
    roles_count: int = Field(default=0, description="Number of roles")
    appointments_count: int = Field(default=0, description="Number of appointments")

    # Integrity hashes
    companies_sha256: Optional[str] = Field(None, description="SHA256 of companies.csv")
    people_sha256: Optional[str] = Field(None, description="SHA256 of people.csv")
    roles_sha256: Optional[str] = Field(None, description="SHA256 of roles.csv")
    appointments_sha256: Optional[str] = Field(
        None, description="SHA256 of appointments.csv"
    )

    # Processing metadata
    revision_ids: Dict[str, str] = Field(
        default_factory=dict, description="Source revision IDs"
    )
    processing_duration_ms: int = Field(default=0, description="Total processing time")
    errors_count: int = Field(default=0, description="Number of errors encountered")
    warnings_count: int = Field(default=0, description="Number of warnings generated")

    # Governance
    license_notice: str = Field(
        default="Wikipedia content available under CC BY-SA 3.0. Wikidata content available under CC0.",
        description="License attribution",
    )
