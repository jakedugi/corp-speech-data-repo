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

## Installation & Environment

This repo uses Astral UV for dependency and environment management.

```bash
# Create/refresh the virtualenv
uv sync --dev

# Run any command without setting PYTHONPATH
uv run python -c "import corpus_hydrator, corpus_types"
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


### Running Tests
```bash
pytest tests/
```
## Contributions
We welcome contributions

## License

This module is part of the corpus project and follows the same license terms.
