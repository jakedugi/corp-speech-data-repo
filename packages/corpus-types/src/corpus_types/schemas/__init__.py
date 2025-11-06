"""
Corpus Types Schema Package

This package provides the authoritative data contracts (schemas) for the corpus processing pipeline.
All data exchange between modules should use these Pydantic models for validation and consistency.

Key Principles:
- Single Source of Truth (SSOT) for all data schemas
- Versioned schemas with semantic versioning
- Comprehensive validation and type safety
- Extensible design for future requirements

Schema Governance:
- All schemas are versioned using Semantic Versioning (SemVer)
- Breaking changes require major version bumps
- Schema validation is enforced at runtime
- JSON schemas can be exported for external validation
"""

# Core schema exports
from .models import (
    # Base types
    StrictBase,
    ExtensibleBase,
    Meta,
    SchemaVersion,

    # Provenance types
    RequestProv,
    ResponseProv,
    AdapterProv,
    Producer,
    CourtListenerProv,
    Provenance,
    Span,

    # Configuration types
    APIConfig,
    CourtListenerConfig,

    # Document types
    Doc,
    Quote,
    Outcome,
    CashAmountCandidate,

    # Legacy types
    QuoteCandidate,

    # Index constituent types
    IndexConstituent,
    IndexExtractionResult,
    IndexConstituentFilter,

    # Wikipedia Key People models
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

    # Configuration functions
    get_default_config,
    get_sp500_config,
    get_multi_index_config,
    validate_config,
    validate_key_person,

    # Constants
    NORMALIZED_ROLE_VOCABULARY,
    wikipedia_key_people_version,
)

# Specialized schema exports
from .base_types import APIConfig as LegacyAPIConfig
from .quote_candidate import QuoteCandidate as LegacyQuoteCandidate
from .scraper import *
from .wikipedia_key_people import *

# Schema version information
__version__ = "2.0.0"

__all__ = [
    # Version info
    "__version__",

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
    "CourtListenerConfig",
    "LegacyAPIConfig",

    # Document types
    "Doc",
    "Quote",
    "Outcome",
    "CashAmountCandidate",

    # Legacy types
    "QuoteCandidate",
    "LegacyQuoteCandidate",

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
