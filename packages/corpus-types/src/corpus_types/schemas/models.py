"""
Pydantic models for corpus-types data contracts.

This module defines the authoritative data models used throughout the corpus processing pipeline.
These models serve as contracts for data exchange between different modules and provide
validation and serialization capabilities.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Literal, Union
from pydantic import BaseModel, Field, validator, ConfigDict


# --------------------------------------------------------------------------- #
# Schema Version and Base Types                                              #
# --------------------------------------------------------------------------- #

SchemaVersion = Literal["1.0"]


class StrictBase(BaseModel):
    """Base class for strict models that forbid extra fields."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class ExtensibleBase(BaseModel):
    """Base class for extensible models that allow extra fields."""

    model_config = ConfigDict(extra="allow", validate_assignment=True)


class Meta(ExtensibleBase):
    """Metadata container for various entities."""

    source: Optional[str] = None
    court: Optional[str] = None
    docket: Optional[str] = None
    party: Optional[str] = None


# --------------------------------------------------------------------------- #
# Provenance Types                                                           #
# --------------------------------------------------------------------------- #


class RequestProv(StrictBase):
    """Request provenance information."""

    endpoint: str = Field(..., description="API endpoint used")
    params_hash: str = Field(..., description="Hash of normalized request parameters")


class ResponseProv(StrictBase):
    """Response provenance information."""

    http_status: Optional[int] = Field(None, description="HTTP status code")
    sha256: str = Field(..., description="SHA256 checksum of response payload")
    bytes: Optional[int] = Field(None, description="Response size in bytes")
    content_type: Optional[str] = Field(None, description="Response content type")

    @validator("sha256")
    def validate_sha256_format(cls, v: str) -> str:
        """Validate SHA256 is 64 hex characters."""
        import re

        if not re.match(r"^[a-f0-9]{64}$", v):
            raise ValueError("sha256 must be a valid 64-character hexadecimal string")
        return v

    @validator("http_status")
    def validate_http_status(cls, v: Optional[int]) -> Optional[int]:
        """Validate HTTP status code is in valid range."""
        if v is not None and not (100 <= v <= 599):
            raise ValueError("http_status must be between 100 and 599")
        return v


class AdapterProv(StrictBase):
    """Adapter provenance information."""

    name: Literal["corpus-hydrator"] = Field(..., description="Adapter name")
    version: str = Field(..., description="Adapter version")
    git_sha: Optional[str] = Field(None, description="Git SHA of adapter")


class Producer(StrictBase):
    """Producer information for derived artifacts."""

    name: Literal[
        "corpus-cleaner",
        "corpus-extractors",
        "corpus-features",
        "corpus-aggregator",
        "corpus-temporal-cv",
    ] = Field(..., description="Producer name")
    version: str = Field(..., description="Producer version")
    git_sha: Optional[str] = Field(None, description="Git SHA of producer")
    params_hash: Optional[str] = Field(None, description="Hash of producer parameters")
    run_id: Optional[str] = Field(None, description="Unique run identifier")
    timestamp: datetime = Field(
        default_factory=datetime.now, description="Production timestamp"
    )


class CourtListenerProv(StrictBase):
    """CourtListener-specific provenance information."""

    source: Literal["courtlistener"] = Field(
        default="courtlistener", description="Source type"
    )
    opinion_id: Optional[int] = Field(None, description="CourtListener opinion ID")
    cluster_id: Optional[int] = Field(None, description="CourtListener cluster ID")
    docket_id: Optional[int] = Field(None, description="CourtListener docket ID")
    absolute_url: Optional[str] = Field(
        None, description="Absolute URL to document page"
    )
    download_url: Optional[str] = Field(None, description="Download URL for document")
    citation: Optional[str] = Field(None, description="Primary citation")
    docket_number: Optional[str] = Field(None, description="Docket number")
    md5: Optional[str] = Field(None, description="MD5 checksum if provided")
    sha1: Optional[str] = Field(None, description="SHA1 checksum if provided")


class Provenance(StrictBase):
    """Complete provenance information for a document."""

    # Generic fields (always present)
    source: Literal["courtlistener", "pacer", "scrape", "manual"] = Field(
        ..., description="Data source"
    )
    source_uri: str = Field(..., description="Canonical source URI")
    retrieved_at: datetime = Field(..., description="When document was retrieved")
    request: RequestProv = Field(..., description="Request provenance")
    response: ResponseProv = Field(..., description="Response provenance")
    adapter: AdapterProv = Field(..., description="Adapter provenance")

    # Optional generic fields
    api_version: Optional[str] = Field(None, description="Upstream API version")
    license: Optional[str] = Field(None, description="Data license")

    # Provider-specific fields (namespaced)
    provider: Optional[CourtListenerProv] = Field(
        None, description="Provider-specific provenance"
    )

    @validator("source")
    def validate_source_matches_provider(cls, v: str, values: Dict[str, Any]) -> str:
        """Ensure provider type matches source when provider is present."""
        if "provider" in values and values["provider"] is not None:
            if v == "courtlistener" and values["provider"].source != "courtlistener":
                raise ValueError("Provider source must match document source")
        return v

    @validator("provider")
    def validate_provider_consistency(
        cls, v: Optional[CourtListenerProv], values: Dict[str, Any]
    ) -> Optional[CourtListenerProv]:
        """Validate provider-specific requirements."""
        if v is None:
            return v

        # For CourtListener, ensure at least one ID is present
        if v.source == "courtlistener":
            has_id = any(
                [
                    v.opinion_id is not None,
                    v.cluster_id is not None,
                    v.docket_id is not None,
                ]
            )
            if not has_id:
                raise ValueError(
                    "CourtListener provider must have at least one of: opinion_id, cluster_id, docket_id"
                )

        return v


class Span(StrictBase):
    """Text span with start/end positions."""

    start: int = Field(ge=0)
    end: int = Field(ge=0)  # exclusive
    # Optional mapping when normalization changes offsets
    offset_map_id: Optional[str] = None

    @validator("end")
    def validate_end_after_start(cls, v: int, values: Dict[str, Any]) -> int:
        """Ensure end >= start."""
        if "start" in values and v < values["start"]:
            raise ValueError("end must be >= start")
        return v


# --------------------------------------------------------------------------- #
# Base Configuration Types                                                    #
# --------------------------------------------------------------------------- #


class APIConfig(ExtensibleBase):
    """Generic API configuration container."""

    api_token: Optional[str] = None
    rate_limit: float = 0.25

    @property
    def api_key(self) -> Optional[str]:
        """Alias for api_token for backward compatibility."""
        return self.api_token


# --------------------------------------------------------------------------- #
# Document Types                                                              #
# --------------------------------------------------------------------------- #


class Doc(StrictBase):
    """
    Document model representing a raw document from any source.

    This is the primary data contract for documents fetched from APIs or scraped
    from web sources. It contains all the raw text and metadata needed for
    downstream processing.
    """

    schema_version: SchemaVersion = "1.0"
    doc_id: str = Field(..., description="Unique document identifier")
    source_uri: str = Field(..., description="Original source URI")
    retrieved_at: datetime = Field(
        default_factory=datetime.now, description="When document was retrieved"
    )
    raw_text: str = Field(..., description="Raw document text content")
    meta: Meta = Field(default_factory=Meta, description="Document metadata")
    provenance: Provenance = Field(..., description="Complete provenance information")

    @validator("doc_id")
    def validate_doc_id(cls, v: str) -> str:
        """Validate that doc_id is not empty."""
        if not v or not v.strip():
            raise ValueError("doc_id cannot be empty")
        return v.strip()

    @validator("raw_text")
    def validate_raw_text(cls, v: str) -> str:
        """Validate that raw_text is not empty."""
        if not v or not v.strip():
            raise ValueError("raw_text cannot be empty")
        return v.strip()


# --------------------------------------------------------------------------- #
# Quote Types                                                                 #
# --------------------------------------------------------------------------- #


class Quote(ExtensibleBase):
    """
    Quote model representing core extracted fields from a document.

    This model contains only the essential pre-ML fields needed for downstream
    processing and modeling. All ML features, embeddings, and predictions are excluded.
    """

    schema_version: SchemaVersion = "1.0"
    quote_id: Optional[str] = Field(None, description="Unique quote identifier")

    # Core identifiers
    doc_id: str = Field(..., description="Document this quote belongs to")
    case_id: Optional[str] = Field(None, description="Case identifier")
    case_id_clean: Optional[str] = Field(None, description="Clean case identifier")
    case_year: Optional[int] = Field(None, description="Case year")
    record_id: Optional[int] = Field(None, description="Record identifier")

    # Provenance fields
    court: Optional[str] = Field(None, description="Court name (extracted from doc_id or metadata)")
    law: Optional[str] = Field(None, description="Law type (extracted from query or content)")
    company: Optional[str] = Field(None, description="Company name (extracted from search or headers)")

    # Core quote content
    text: str = Field(..., description="Quote text content")
    context: Optional[str] = Field(None, description="Surrounding context")
    speaker: Optional[str] = Field(None, description="Speaker of the quote")
    score: Optional[float] = Field(None, description="Confidence/extraction score")
    urls: List[str] = Field(default_factory=list, description="Source URLs")

    # Position and structural features
    docket_number: Optional[float] = Field(None, description="Docket number")
    docket_token_start: Optional[float] = Field(
        None, description="Token start position in docket"
    )
    docket_char_start: Optional[float] = Field(
        None, description="Character start in docket"
    )
    global_token_start: Optional[float] = Field(
        None, description="Global token start position"
    )
    global_char_start: Optional[float] = Field(
        None, description="Global character start"
    )
    num_tokens: Optional[float] = Field(None, description="Number of tokens")

    # Case and metadata
    case_id: Optional[str] = Field(None, description="Case identifier")
    case_id_clean: Optional[str] = Field(None, description="Clean case identifier")
    case_year: Optional[int] = Field(None, description="Case year")

    # Hash and identifiers
    text_hash: Optional[str] = Field(None, description="Text hash")
    text_hash_norm: Optional[str] = Field(None, description="Normalized text hash")
    urls: List[str] = Field(default_factory=list, description="Source URLs")

    # Outcome field (pre-ML)
    final_judgement_real: Optional[float] = Field(
        None, description="Final judgment value"
    )

    # Internal processing metadata
    _metadata_src_path: Optional[str] = Field(None, description="Metadata source path")
    _metadata_wrapped: Optional[bool] = Field(
        None, description="Whether metadata is wrapped"
    )
    _leakage_prevention: Dict[str, Any] = Field(
        default_factory=dict, description="Leakage prevention metadata"
    )

    @validator("quote_id")
    def validate_quote_id(cls, v: Optional[str]) -> Optional[str]:
        """Validate that quote_id is not empty if provided."""
        if v is not None and (not v or not v.strip()):
            raise ValueError("quote_id cannot be empty if provided")
        return v.strip() if v else None

    @validator("doc_id")
    def validate_doc_id(cls, v: str) -> str:
        """Validate that doc_id is not empty."""
        if not v or not v.strip():
            raise ValueError("doc_id cannot be empty")
        return v.strip()

    @validator("text")
    def validate_text(cls, v: str) -> str:
        """Validate that text is not empty."""
        if not v or not v.strip():
            raise ValueError("text cannot be empty")
        return v.strip()


# --------------------------------------------------------------------------- #
# Outcome Types                                                               #
# --------------------------------------------------------------------------- #


class Outcome(StrictBase):
    """
    Outcome model representing case outcomes and labels.

    This model captures the labeled outcomes for legal cases, including
    the source of the label and any additional metadata.
    """

    schema_version: SchemaVersion = "1.0"
    case_id: str = Field(..., description="Unique case identifier")
    label: Literal["win", "loss", "settlement", "dismissal", "mixed", "unknown"] = (
        Field(..., description="Outcome label")
    )
    label_source: Literal["manual", "heuristic", "external", "inferred"] = Field(
        ..., description="Source of the label"
    )
    date: Optional[datetime] = Field(None, description="Date of the outcome")
    meta: Meta = Field(default_factory=Meta, description="Outcome metadata")

    @validator("case_id")
    def validate_case_id(cls, v: str) -> str:
        """Validate that case_id is not empty."""
        if not v or not v.strip():
            raise ValueError("case_id cannot be empty")
        return v.strip()


# --------------------------------------------------------------------------- #
# Removed: Prediction Types (ML outputs excluded from pre-ML schema)      #
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Removed: Feature Types (ML features excluded from pre-ML schema)         #
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Legacy Types (for backward compatibility)                                   #
# --------------------------------------------------------------------------- #


class QuoteCandidate(ExtensibleBase):
    """
    Legacy quote candidate model for backward compatibility.

    This model represents a potential quote during the extraction process
    and contains only the essential pre-ML fields.
    """

    quote: str = Field(..., description="Quote text")
    context: str = Field(..., description="Surrounding context")
    urls: List[str] = Field(default_factory=list, description="Source URLs")
    speaker: Optional[str] = Field(None, description="Detected speaker")
    score: float = Field(default=0.0, description="Confidence score")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "text": self.quote,
            "speaker": self.speaker,
            "score": self.score,
            "urls": self.urls,
            "context": self.context,
        }


# --------------------------------------------------------------------------- #
# Index Constituents Types                                                    #
# --------------------------------------------------------------------------- #


class IndexConstituent(StrictBase):
    """
    Data model for an index constituent (company in a market index).

    This model represents a company that is part of a market index such as
    S&P 500, Dow Jones Industrial Average, or Nasdaq 100.
    """

    symbol: str = Field(..., description="Stock ticker symbol")
    company_name: str = Field(..., description="Full company name")
    index_name: str = Field(..., description="Name of the market index")
    sector: Optional[str] = Field(None, description="Company sector (if available)")
    industry: Optional[str] = Field(None, description="Company industry (if available)")
    date_added: Optional[str] = Field(None, description="Date company was added to index")
    extracted_at: datetime = Field(
        default_factory=datetime.now, description="When data was extracted"
    )
    source_url: str = Field(..., description="Source Wikipedia URL")

    @validator('symbol')
    def validate_symbol(cls, v):
        """Validate stock ticker symbol format."""
        if not v or not v.strip():
            raise ValueError("Symbol cannot be empty")

        # Clean the symbol
        v = v.strip().upper()

        # Basic validation - should be 1-5 uppercase letters, optionally with a dot and more letters
        if not re.match(r'^[A-Z]{1,5}(\.[A-Z]{1,2})?$', v):
            # Allow some special cases like numbers in tickers
            if not re.match(r'^[A-Z0-9]{1,5}(\.[A-Z0-9]{1,2})?$', v):
                raise ValueError(f"Invalid symbol format: {v}")

        return v

    @validator('company_name')
    def validate_company_name(cls, v):
        """Validate company name."""
        if not v or not v.strip():
            raise ValueError("Company name cannot be empty")
        return v.strip()

    @validator('date_added')
    def validate_date_added(cls, v):
        """Validate date added format."""
        if v is None:
            return v

        # Try to parse various date formats
        date_formats = [
            '%Y-%m-%d',  # 2023-12-15
            '%m/%d/%Y',  # 12/15/2023
            '%B %d, %Y', # December 15, 2023
            '%b %d, %Y', # Dec 15, 2023
        ]

        for fmt in date_formats:
            try:
                datetime.strptime(v, fmt)
                return v
            except ValueError:
                continue

        # If no format matches, return as-is but log warning
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Unrecognized date format: {v}")
        return v


class IndexExtractionResult(StrictBase):
    """Result model for index extraction operations."""

    index_name: str
    total_constituents: int
    extracted_at: datetime = Field(default_factory=datetime.now)
    success: bool = True
    error_message: Optional[str] = None
    constituents: list = Field(default_factory=list)


class IndexConstituentFilter(ExtensibleBase):
    """Filter class for querying index constituents."""

    symbols: Optional[List[str]] = Field(None, description="Filter by stock symbols")
    sectors: Optional[List[str]] = Field(None, description="Filter by sectors")
    industries: Optional[List[str]] = Field(None, description="Filter by industries")
    date_range: Optional[Tuple[datetime, datetime]] = Field(
        None, description="Filter by date added range"
    )

    def matches(self, constituent: IndexConstituent) -> bool:
        """Check if a constituent matches the filter criteria."""
        # Symbol filter
        if self.symbols and constituent.symbol not in self.symbols:
            return False

        # Sector filter
        if self.sectors and constituent.sector not in self.sectors:
            return False

        # Industry filter
        if self.industries and constituent.industry not in self.industries:
            return False

        # Date range filter
        if self.date_range and constituent.date_added:
            try:
                added_date = datetime.strptime(constituent.date_added, '%Y-%m-%d')
                start_date, end_date = self.date_range
                if not (start_date <= added_date <= end_date):
                    return False
            except (ValueError, TypeError):
                pass  # Skip date filtering if parsing fails

        return True


# --------------------------------------------------------------------------- #
# Wikipedia Key People Types (Imported from separate module for governance)  #
# --------------------------------------------------------------------------- #

# Import all Wikipedia key people models to make them available in the main schema
from .wikipedia_key_people import (
    WikipediaKeyPerson,
    WikipediaCompany,
    WikipediaExtractionResult,
    WikipediaScrapingConfig,
    WikipediaContentConfig,
    WikipediaIndexConfig,
    WikipediaKeyPeopleConfig,
    NormalizedCompany,
    NormalizedPerson,
    NormalizedRole,
    NormalizedAppointment,
    DatasetManifest,
    get_default_config,
    get_sp500_config,
    get_multi_index_config,
    validate_config,
    validate_key_person,
    NORMALIZED_ROLE_VOCABULARY,
    __version__ as wikipedia_key_people_version,
)

# Add to main module exports
__all__ = [
    # Base types
    "StrictBase",
    "ExtensibleBase",
    "Meta",
    "SchemaVersion",

    # Provenance types
    "RequestProv",
    "ResponseProv",
    "AdapterProv",
    "Producer",
    "CourtListenerProv",
    "Provenance",
    "Span",

    # Configuration types
    "APIConfig",

    # Document types
    "Doc",
    "Quote",
    "Outcome",

    # Legacy types
    "QuoteCandidate",

    # Index constituent types
    "IndexConstituent",
    "IndexExtractionResult",
    "IndexConstituentFilter",

    # Wikipedia Key People models
    "WikipediaKeyPerson",
    "WikipediaCompany",
    "WikipediaExtractionResult",
    "WikipediaScrapingConfig",
    "WikipediaContentConfig",
    "WikipediaIndexConfig",
    "WikipediaKeyPeopleConfig",
    "NormalizedCompany",
    "NormalizedPerson",
    "NormalizedRole",
    "NormalizedAppointment",
    "DatasetManifest",

    # Configuration functions
    "get_default_config",
    "get_sp500_config",
    "get_multi_index_config",
    "validate_config",
    "validate_key_person",

    # Constants
    "NORMALIZED_ROLE_VOCABULARY",
    "wikipedia_key_people_version",
]
