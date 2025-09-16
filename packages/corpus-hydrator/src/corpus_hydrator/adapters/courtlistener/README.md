# CourtListener Legal Data Collection System

## Overview

The **CourtListener Legal Data Collection System** is a comprehensive, production-ready adapter that provides structured access to CourtListener's legal document APIs. This system enables systematic collection of corporate legal documents, docket information, and case metadata to support legal research, compliance monitoring, and corporate risk assessment workflows.

## Purpose & Importance

### Why This Matters

This system serves as a **critical data collection layer** that powers multiple downstream legal and compliance processes:

1. **Legal Research Enhancement**: Provides systematic access to relevant case law and legal documents
2. **Corporate Compliance Monitoring**: Enables tracking of legal proceedings involving specific companies
3. **Risk Assessment Support**: Supplies data for analyzing legal risks and regulatory exposure
4. **NER Model Training**: Provides high-quality labeled legal document data for training models
5. **Regulatory Intelligence**: Supports monitoring of industry-wide legal developments and trends

### Accuracy Expectations

**Important**: This system provides **high-quality, structured legal data** with the following quality standards:

- **Data Completeness**: Extracts comprehensive case information including dockets, entries, and documents
- **Source Reliability**: Uses authoritative CourtListener API data with proper authentication
- **Structured Output**: Produces relational data with proper legal document relationships
- **Audit Trail**: Includes provenance tracking and API response metadata
- **Rate Limit Compliance**: Respects CourtListener's API limits (5,000 queries/hour)

## Single Source of Truth (SSOT) Compliance

All schemas and models used by this adapter are defined in the `corpus-types` package for SSOT compliance:

- **CourtListenerConfig**: Extends `APIConfig` with CourtListener-specific settings
- **CourtListenerProv**: CourtListener-specific provenance information
- **Provenance**: Complete provenance tracking for all documents
- **APIConfig**: Base API configuration with token and rate limiting

This ensures consistency across all modules and prevents schema drift.

## Architecture Overview

The system follows **Clean Architecture** principles with clear separation of concerns and modular design:

```
CourtListener Legal Data System
├── providers/           # API Clients & Data Sources
│   ├── client.py           # CourtListener REST API client
│   └── __init__.py
├── parsers/             # Query Construction & Parsing
│   ├── query_builder.py    # Legal statute query building
│   └── __init__.py
├── core/                # Core Processing Functions
│   ├── processor.py        # Document processing & RECAP handling
│   └── __init__.py
├── normalize.py         # Data Standardization (Future)
├── usecase.py           # Business Logic & Orchestration
├── writer.py            # Data Output & Serialization
├── config.py            # Configuration Management
├── cli/                 # Command Line Interface
│   ├── commands.py         # CLI command definitions
│   └── __init__.py
├── utils/               # Shared Utilities
│   ├── file_io.py          # File operations & downloads
│   ├── http_utils.py       # HTTP client with rate limiting
│   └── __init__.py
└── Comprehensive testing  # Unit tests & validation
```

## Data Flow

1. **Query Building** → Construct legal statute queries with company filtering
2. **API Search** → Execute searches against CourtListener's search API
3. **Result Processing** → Parse and filter search results
4. **Docket Hydration** → Fetch complete docket information for each case
5. **Document Collection** → Download court documents and attachments
6. **Data Persistence** → Save structured data with provenance tracking

## Key Features

- **Statute-Based Queries**: Predefined legal queries for major corporate speech statutes (FTC, Lanham Act, SEC Rule 10b-5)
- **Company Chunking**: Handles URL length limits by intelligently chunking company names
- **Multi-Step Hydration**: Complete docket processing pipeline (search → entries → documents → attachments)
- **Async Processing**: Parallel document fetching with configurable concurrency control
- **Rate Limit Management**: Respects CourtListener's 5,000 queries/hour limit with exponential backoff
- **Graceful Error Handling**: Handles API errors, timeouts, and rate limiting automatically
- **Structured Output**: JSON files with complete metadata and provenance tracking
- **CLI Interface**: Comprehensive command-line tools for production and testing workflows
- **Clean Architecture**: Modular design following strict separation of concerns

## Usage

### Programmatic Usage

```python
from corpus_hydrator.adapters.courtlistener import CourtListenerUseCase

# Configure and run
usecase = CourtListenerUseCase(
    statutes=["FTC Section 5 (9th Cir.)"],
    company_file="data/sp500_constituents.csv",
    chunk_size=10
)
usecase.run()
```

### CLI Usage

```bash
# Full production run (requires .env with credentials)
source .env && uv run python -m corpus_hydrator.cli.cli_courtlistener orchestrate \
  --statutes "FTC Section 5" \
  --company-file data/dowjonesindustrialaverage_constituents.csv \
  --token $COURTLISTENER_API_TOKEN \
  --pages 10 \
  --page-size 50

# Test mode (2 cases max, auto-generated directory)
source .env && uv run python -m corpus_hydrator.cli.cli_courtlistener orchestrate \
  --test-mode

# Limited run (custom case limit)
source .env && uv run python -m corpus_hydrator.cli.cli_courtlistener orchestrate \
  --statutes "FTC Section 5" \
  --company-file data/dowjonesindustrialaverage_constituents.csv \
  --token $COURTLISTENER_API_TOKEN \
  --max-cases 5
```

## Configuration

### Environment Variables

```bash
# Required
COURTLISTENER_API_TOKEN=your_api_token_here

# Optional - Rate Limiting (respects CourtListener's 5K/hour limit)
COURTLISTENER_ASYNC_RATE_LIMIT=3.0  # Default: 3.0 seconds between requests

# Optional - Output Configuration
COURTLISTENER_OUTPUT_DIR=data/raw/courtlistener  # Default output directory

# Optional - Processing Configuration
COURTLISTENER_DISABLE_PDF_DOWNLOADS=false  # Set to true to skip PDF downloads

# Optional - PACER Credentials (for full document access)
PACER_USER=your_pacer_username
PACER_PASS=your_pacer_password
```

### Configuration Classes

The system uses SSOT-compliant configuration classes from `corpus-types`:

- **CourtListenerConfig**: Extends `APIConfig` with CourtListener-specific settings
- **CourtListenerProv**: Provenance tracking for CourtListener data
- **Provenance**: Complete audit trail for all collected documents

## Testing

The system includes comprehensive testing with multiple test modes:

### Test Modes

```bash
# Quick test (2 cases max, auto-generated timestamped directory)
source .env && uv run python -m corpus_hydrator.cli.cli_courtlistener orchestrate --test-mode

# Custom case limit
source .env && uv run python -m corpus_hydrator.cli.cli_courtlistener orchestrate --max-cases 5

# Dry run (no actual API calls)
# Set COURTLISTENER_DISABLE_PDF_DOWNLOADS=true for faster testing
```

### Test Structure

```
tests/unit/courtlistener/
├── test_config.py           # Configuration testing
├── test_courtlistener_client.py  # API client testing
├── test_orchestrator.py     # Usecase testing
├── test_query_builder.py    # Query construction testing
├── test_queries.py          # Statute query testing
├── test_docket_api.py       # Docket API testing
└── test_ftc_client.py       # FTC-specific testing
```

### Running Tests

```bash
# Run all CourtListener tests
cd packages/corpus-hydrator && python -m pytest tests/unit/courtlistener/ -v

# Run specific test file
cd packages/corpus-hydrator && python -m pytest tests/unit/courtlistener/test_config.py -v

# Run with coverage
cd packages/corpus-hydrator && python -m pytest tests/unit/courtlistener/ --cov=corpus_hydrator.adapters.courtlistener
```

## Supported Statutes

The adapter includes predefined queries for major corporate speech statutes:

- FTC Section 5 (9th Cir.)
- FTC Section 12
- Lanham Act § 43(a)
- SEC Rule 10b-5
- SEC Regulation FD
- NLRA § 8(a)(1)
- CFPA UDAAP
- California § 17200 / 17500
- NY GBL §§ 349–350
- FD&C Act § 331

## Data Flow

1. **Query Building**: Statute templates combined with company names
2. **Chunking**: Company lists split to avoid URL length limits
3. **Search**: CourtListener `/search/` endpoint for docket discovery
4. **Hydration**: Full docket data retrieval (entries, documents, PDFs)
5. **Output**: Structured data saved to organized directory hierarchy

## Integration Points

- **Index Constituents**: CSV files with `official_name` column
- **Query Templates**: Statute-specific search strings in `STATUTE_QUERIES`
- **Rate Limiting**: Built-in delays and retry logic
- **Error Handling**: Robust error handling with exponential backoff

## Migration from Old Structure

This unified adapter replaces the previous scattered CourtListener code:

- `courtlistener_core.py` → `core/processor.py`
- `courtlistener_client.py` → `providers/client.py`
- `queries.py` → `parsers/query_builder.py`
- `courtlistener_orchestrator.py` → `usecase.py`
- `cli_courtlistener.py` → `cli/commands.py`

All existing functionality is preserved with improved organization and maintainability.
