# Wikipedia Market Index Scraper

A configurable, extensible scraper for extracting company and executive information from Wikipedia market index pages and SEC EDGAR filings.

## Features

- **Multi-Index Support**: S&P 500, Dow Jones, NASDAQ-100, Russell 1000, and custom indices
- **Dual Data Sources**: Wikipedia infoboxes + SEC EDGAR filings
- **Configurable Rate Limiting**: Respects API limits with automatic backoff
- **Extensible Architecture**: Easy to add new indices or data sources
- **Schema-Driven**: Configuration controlled by authoritative schemas
- **Comprehensive Output**: Wide and long format CSV files
- **Robust Error Handling**: Graceful degradation and retry logic

## Quick Start

### Command Line Usage

```bash
# Scrape S&P 500 companies and executives
hydrator fetch wikipedia --index sp500 --output-dir data/

# Scrape with custom limits
hydrator fetch wikipedia --index sp500 --max-companies 50 --output-dir data/

# Dry run for testing
hydrator fetch wikipedia --index sp500 --dry-run --verbose

# Scrape different index
hydrator fetch wikipedia --index dow --output-dir data/
```

### Python API Usage

```python
from corpus_hydrator.adapters.wikipedia.scraper import WikipediaScraper
from corpus_types.schemas.scraper import get_default_config

# Create scraper with default config
config = get_default_config()
config.enabled_indices = ["sp500"]
scraper = WikipediaScraper(config)

# Scrape companies
companies, result = scraper.scrape_index("sp500")
print(f"Found {len(companies)} companies")

# Scrape executives
officers = scraper.scrape_executives_for_companies(companies)
print(f"Found executives for {len(officers)} companies")

# Save results
scraper.save_results(companies, officers, "sp500")
```

## Supported Indices

| Index | Command | Description |
|-------|---------|-------------|
| S&P 500 | `--index sp500` | Standard & Poor's 500 large companies |
| Dow Jones | `--index dow` | Dow Jones Industrial Average (30 companies) |
| NASDAQ-100 | `--index nasdaq100` | Technology-heavy index (100 companies) |
| Russell 1000 | `--index russell1000` | Broad market index (1000 companies) |

## Output Files

The scraper generates three types of output files:

### 1. Company List (`{index}_aliases.csv`)
Basic company information:
```csv
ticker,official_name,cik,wikipedia_url,index_name
AAPL,Apple Inc.,0000320193,https://en.wikipedia.org/wiki/Apple_Inc.,sp500
MSFT,Microsoft Corporation,0000789019,https://en.wikipedia.org/wiki/Microsoft,sp500
```

### 2. Wide Format (`{index}_aliases_enriched.csv`)
One row per company with executives as columns:
```csv
ticker,official_name,cik,exec1,exec2,exec3,...
AAPL,Apple Inc.,0000320193,Tim Cook (CEO),Luca Maestri (CFO),...
```

### 3. Long Format (`{index}_officers_cleaned.csv`)
One row per executive:
```csv
ticker,official_name,cik,name,title,source,scraped_at
AAPL,Apple Inc.,0000320193,Tim Cook,CEO,wikipedia,2024-01-01T10:00:00
AAPL,Apple Inc.,0000320193,Luca Maestri,CFO,sec_edgar,2024-01-01T10:00:00
```

## Configuration

### YAML Configuration File

Create a `wikipedia_scraper.yaml` file:

```yaml
version: "1.0.0"
enabled_indices: ["sp500", "dow"]

scraping:
  wikipedia_rate_limit: 1.0
  sec_rate_limit: 10.0
  max_people_per_company: 100
  max_companies: 100  # For testing

extraction:
  role_keywords:
    - "key people"
    - "executive officers"
    - "leadership"

indices:
  sp500:
    name: "S&P 500"
    wikipedia_url: "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    table_id: "constituents"
    ticker_column: 0
    name_column: 1
    cik_column: 6
```

### Adding Custom Indices

To add a custom index, extend the configuration:

```yaml
indices:
  custom_tech:
    name: "Custom Technology Index"
    short_name: "custom_tech"
    wikipedia_url: "https://en.wikipedia.org/wiki/List_of_technology_companies"
    table_id: "companies"
    ticker_column: 0
    name_column: 1
    cik_column: 2
    max_companies: 50
```

## Architecture

### Separation of Concerns (SOC)

The scraper follows clean SOC principles:

1. **Configuration Layer** (`corpus_types.schemas.scraper`)
   - Authoritative schemas for all scraper behavior
   - Validation and type safety
   - Index-specific configurations

2. **HTTP Layer** (`HTTPClient`, `RateLimiter`)
   - Rate limiting and retry logic
   - Connection pooling
   - Service-specific adapters

3. **Scraping Layer** (`WikipediaIndexScraper`, `WikipediaPeopleScraper`, `SECEdgarScraper`)
   - Data extraction logic
   - Source-specific parsing
   - Error handling and fallbacks

4. **Processing Layer** (`WikipediaScraper`)
   - Orchestrates all scraping operations
   - Data merging and deduplication
   - Output formatting

### KISS Principles

- **Simple Configuration**: YAML-driven, no code changes needed for new indices
- **Single Responsibility**: Each class has one clear purpose
- **Fail Fast**: Early validation and clear error messages
- **Sensible Defaults**: Works out-of-the-box with minimal configuration

## Data Quality & Validation

### Automatic Validation

The scraper includes comprehensive validation:

```python
# CIK format validation
assert cik_pattern.match(cik), f"Invalid CIK: {cik}"

# Ticker format validation
assert ticker_pattern.match(ticker), f"Invalid ticker: {ticker}"

# Data quality checks
assert len(officers) >= min_officers_required, "Insufficient officer data"
```

### Error Handling

- **Network Failures**: Automatic retries with exponential backoff
- **Missing Data**: Fallback parsing strategies
- **Malformed HTML**: Graceful degradation to text-based extraction
- **Rate Limits**: Built-in rate limiting and queue management

## Testing

### Unit Tests

```bash
# Run scraper tests
pytest packages/corpus-hydrator/tests/test_wikipedia_scraper.py

# Run with coverage
pytest --cov=corpus_hydrator.adapters.wikipedia packages/corpus-hydrator/tests/
```

### Integration Tests

```python
def test_full_scraping_workflow():
    config = get_default_config()
    config.dry_run = True

    scraper = WikipediaScraper(config)
    companies, result = scraper.scrape_index("sp500")

    assert len(companies) > 0
    assert result.errors == []
```

## Extending for New Indices

### 1. Add Index Configuration

```yaml
indices:
  new_index:
    name: "New Market Index"
    short_name: "new_index"
    wikipedia_url: "https://en.wikipedia.org/wiki/New_Index"
    table_id: "companies"
    ticker_column: 0
    name_column: 1
    cik_column: 2
```

### 2. Test the Configuration

```bash
hydrator fetch wikipedia --index new_index --dry-run --verbose
```

### 3. Handle Special Cases

If the new index has a different HTML structure, extend the `WikipediaIndexScraper` class:

```python
class CustomIndexScraper(WikipediaIndexScraper):
    def _extract_ticker(self, cell):
        # Custom ticker extraction logic
        return super()._extract_ticker(cell)
```

## Performance Considerations

### Rate Limiting
- **Wikipedia**: 1 request/second (configurable)
- **SEC EDGAR**: 10 requests/second (configurable)
- **Automatic Backoff**: Exponential backoff on failures

### Parallel Processing
- **Company Scraping**: Parallel execution with configurable workers
- **Connection Pooling**: HTTP connection reuse for efficiency

### Memory Management
- **Streaming Processing**: Large datasets processed without loading everything into memory
- **Batch Processing**: Configurable batch sizes for large indices

## Troubleshooting

### Common Issues

1. **Rate Limiting Errors**
   ```python
   config.scraping.wikipedia_rate_limit = 0.5  # Reduce rate
   ```

2. **Missing CIK Data**
   ```yaml
   validation:
     require_cik: false  # Allow missing CIKs
   ```

3. **HTML Structure Changes**
   - The scraper includes fallback text extraction
   - Monitor Wikipedia page structure changes
   - Update role keywords as needed

### Debugging

```bash
# Enable verbose logging
hydrator fetch wikipedia --index sp500 --verbose

# Dry run mode
hydrator fetch wikipedia --index sp500 --dry-run
```

## Future Enhancements

- **Machine Learning Integration**: Use NLP models for better officer extraction
- **Real-time Updates**: Monitor Wikipedia for changes
- **Additional Data Sources**: Bloomberg, Reuters, company websites
- **Caching Layer**: Persistent caching for faster re-runs
- **Webhooks**: Real-time notifications for new data

---

The scraper provides a robust, extensible foundation for extracting corporate executive data from Wikipedia and SEC sources, with clean separation of concerns and comprehensive configuration options.
