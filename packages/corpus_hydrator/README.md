# Corpus API

Data collection and API client module for the corporate speech risk dataset. This module provides unified access to multiple legal data sources including CourtListener, RSS feeds, and Wikipedia.

## Overview

The corpus-api module serves as the data ingestion layer for the corpus pipeline, providing:

- **CourtListener API**: Access to court opinions and case data
- **RSS Feeds**: SEC, FTC, and DOJ news feeds
- **Wikipedia Scraping**: Legal case lists and corporate law content
- **Unified Interface**: Consistent data format across all sources
- **Rate Limiting**: Respectful API usage with built-in rate limiting
- **Error Handling**: Robust retry logic and error recovery

## Installation

```bash
pip install -e .
```

## Quick Start

### CourtListener API
```bash
# Set your API key
export COURT_LISTENER_API_KEY="your-api-key-here"

# Fetch documents
corpus-fetch courtlistener --query configs/query.example.yaml --output data/docs.jsonl
```

### RSS Feeds
```bash
# Fetch from RSS feeds
corpus-fetch rss --feeds configs/sources/rss.yaml --output data/rss_docs.jsonl
```

### Wikipedia
```bash
# Scrape Wikipedia pages
corpus-fetch wikipedia --pages configs/sources/wikipedia.yaml --output data/wiki_docs.jsonl
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

## Module Structure

```
corpus_api/
├── cli/
│   └── fetch.py              # Main CLI interface
├── adapters/
│   ├── courtlistener/        # CourtListener API client
│   ├── rss/                  # RSS feed client
│   └── wikipedia/            # Wikipedia scraper
├── client/                   # Base HTTP client classes
├── config/                   # Configuration loading
├── orchestrators/            # High-level orchestration
├── scripts/                  # Utility scripts
├── tests/                    # Test suite
├── configs/                  # Configuration files
├── fixtures/                 # Test fixtures
├── schemas/                  # JSON schemas
└── notebooks/                # Exploration notebooks
```

## API Clients

### CourtListener Client
```python
from corpus_api.adapters.courtlistener.courtlistener_client import CourtListenerClient

client = CourtListenerClient(api_key="your-key")
documents = client.search_opinions({
    "courts": ["scotus"],
    "date_range": {"start": "2020-01-01", "end": "2023-12-31"}
})
```

### RSS Client
```python
from corpus_api.adapters.rss.rss_client import RSSClient

client = RSSClient()
documents = client.fetch_feed("https://www.sec.gov/rss/news/press")
```

### Wikipedia Scraper
```python
from corpus_api.adapters.wikipedia.sandp_scraper import WikipediaScraper

scraper = WikipediaScraper()
content = scraper.scrape_page("Corporate_law")
```

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

## Logging

Uses Loguru for structured logging:

```python
from loguru import logger

logger.info("Fetched {count} documents from {source}", count=len(documents), source="courtlistener")
```

## License

This module is part of the corpus project and follows the same license terms.
