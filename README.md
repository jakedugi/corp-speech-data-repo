# Corporate Speech Data Repository

!Work in progress CI red untill refactor complete!

Data collection, cleaning, and extraction pipeline for corporate speech legal risk examples.

This repository contains the data-focused modules that handle:
- **API clients** for legal data sources
- **Data cleaning** and normalization
- **Quote and outcome extraction**
- **Data type definitions** and schemas

## Modules

### corpus_hydrator/
Data ingestion and scraping (CourtListener, RSS, Wikipedia). Includes the index constituents subsystem and the primary CLI (`hydrator`).

### corpus_cleaner/
Document normalization and offset mapping utilities.

### corpus_extractors/
Quote extraction, attribution, and case outcome imputation.

### corpus_types/
Single source of truth for schemas, IDs, and governance (Pydantic models shared across packages).

## Data Pipeline Overview

- **CourtListener API**: Access to court opinions and case data
- **RSS Feeds**: SEC, FTC, and DOJ news feeds
- **Wikipedia Scraping**: Legal case lists and corporate law content
- **Unified Interface**: Consistent data format across all sources
- **Rate Limiting**: Respectful API usage with built-in rate limiting
- **Error Handling**: Robust retry logic and error recovery

## Installation & Environment

This repo uses Astral UV for dependency and environment management.

```bash
# Create/refresh the virtualenv
uv sync --dev

# Run any command without setting PYTHONPATH
uv run python -c "import corpus_hydrator, corpus_types; print('ok')"
```

## Quick Start (CLI)

All commands run via the `hydrator` CLI.

```bash
# Show commands
uv run hydrator --help

# CourtListener (set your API key first)
export COURT_LISTENER_API_KEY="your-api-key-here"
uv run hydrator courtlistener --config packages/corpus-hydrator/configs/query.example.yaml --output data/docs.jsonl

# Wikipedia: Index constituents (authoritative lists)
uv run hydrator index-constituents --index sp500 --formats csv parquet --output-dir data/ --verbose

# Wikipedia: Executives (dry run)
uv run hydrator wikipedia --index sp500 --dry-run --verbose --output-dir data/
```

## Configuration

### Query Configuration
Create a `query.yaml` file to specify what data to fetch:

```yaml
sources:
  - courtlistener

courtlistener:
  date_range:
    start: "2020-01-01"
    end: "2023-12-31"
  courts: ["scotus", "ca1"]
  keywords: ["corporate", "securities"]
```

### Source Configuration
Configure API endpoints and authentication in `sources/`:

```yaml
# configs/sources/courtlistener.yaml
api:
  base_url: "https://www.courtlistener.com/api/rest/v4"
  timeout: 30

authentication:
  api_key_env_var: "COURT_LISTENER_API_KEY"
```

## Repository Layout (high-level)

```
packages/
  corpus-hydrator/
    src/corpus_hydrator/
      adapters/
        index_constituents/   # providers/, parsers/, normalize.py, usecase.py, writer.py, utils/
      cli/fetch.py            # hydrator CLI entry
    configs/                  # e.g., query.example.yaml
    scripts/                  # package-specific helper scripts

  corpus-extractors/
    src/corpus_extractors/    # extract_quotes.py, case_outcome_imputer.py, etc.

  corpus-cleaner/
    src/corpus_cleaner/       # normalize, offset mapping

  corpus-types/
    src/corpus_types/
      schemas/                # authoritative Pydantic models

docs/                         # project docs
scripts/                      # root orchestration/admin scripts
data/                         # outputs (gitignored except curated samples)
```

## Hydrator CLI (primary entrypoint)

Prefer the CLI for ingestion tasks. See `uv run hydrator --help` for all commands.

## Data Format

All sources produce documents in a unified JSONL format:

```json
{
  "doc_id": "doc_001",
  "source_uri": "https://www.courtlistener.com/opinion/12345/",
  "retrieved_at": "2023-01-01T12:00:00Z",
  "raw_text": "Full document text...",
  "meta": {
    "court": "scotus",
    "docket": "22-123",
    "party": "SEC v. Corporation"
  }
}
```

## Development

### Running Tests
```bash
pytest tests/
```

### Adding New Sources
1. Create adapter in `adapters/`
2. Add CLI command in `cli/fetch.py`
3. Update configuration schemas
4. Add tests and fixtures

### Configuration Validation
The module uses Pydantic for configuration validation:

```python
from pydantic import BaseModel

class QueryConfig(BaseModel):
    sources: List[str]
    courtlistener: Optional[CourtListenerQuery]
    rss: Optional[RSSQuery]
```

## Rate Limiting & Ethics

### Rate Limiting
- CourtListener: 60 requests/minute
- RSS Feeds: 1 request/minute per feed
- Wikipedia: 1 request/second with user agent

### Ethical Usage
- Respect robots.txt for web scraping
- Use appropriate user agents
- Cache responses to reduce API load
- Handle rate limits gracefully

## Error Handling

The module includes comprehensive error handling:

- **Network Errors**: Automatic retry with exponential backoff
- **Rate Limits**: Queue requests and respect limits
- **API Errors**: Structured error messages and recovery
- **Data Validation**: Pydantic models for all inputs/outputs

## Logging

Uses Loguru for structured logging:

```python
from loguru import logger

logger.info("Fetched {count} documents from {source}", count=len(documents), source="courtlistener")
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This module is part of the corpus project and follows the same license terms.
