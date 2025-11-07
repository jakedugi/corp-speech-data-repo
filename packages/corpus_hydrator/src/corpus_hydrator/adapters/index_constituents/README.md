# Index Constituents Extraction System

## Overview

The **Index Constituents Extraction System** is a critical first-step component that provides company data to enhance CourtListener queries and downstream Named Entity Recognition (NER) processes. This system extracts current company lists from major stock indices to help identify and prioritize company mentions in legal documents.

## Purpose & Importance

### Why This Matters

This system serves as a **helper dataset** that significantly improves the performance of downstream processes:

1. **CourtListener Query Enhancement**: Provides company name lists to prioritize relevant legal documents
2. **NER Model Support**: Gives NER systems a prioritized company vocabulary for better entity recognition
3. **Query Optimization**: Helps focus searches on documents most likely to contain company mentions

### Accuracy Expectations

**Important**: This is **NOT** a training dataset and does not need 100% accuracy. The goal is **relevance over perfection**:

- Missing a few companies is acceptable
- Some outdated entries are fine
- The focus is on providing a **useful signal** for downstream processes
- Quality matters more than completeness

## Architecture Overview

The system follows **Clean Architecture** principles with clear separation of concerns:

```
Index Constituents System
├── providers/          # Data Sources
│   ├── wikipedia.py      # Main: Wikipedia scraping
│   ├── fmp.py           # Future: Financial Modeling Prep API
│   └── yahoo_etf.py     # Future: Yahoo Finance ETF data
├── parsers/           # Data Extractors
│   └── html_table.py    # HTML table parsing
├── normalize.py       # Data Standardization
├── usecase.py         # Business Logic
├── writer.py          # Output Generation
├── config.py          # Index Configurations
├── utils/http.py      # HTTP Client with Caching
└── cli/fetch.py       # Command Line Interface
```

## Data Flow

1. **Fetch** → Get raw HTML from data sources
2. **Parse** → Extract tables from HTML
3. **Normalize** → Standardize column names and data types
4. **Validate** → Ensure data quality and completeness
5. **Write** → Generate CSV, Parquet, and manifest files

## Supported Indices

| Index | Companies | Status | Source |
|-------|-----------|--------|--------|
| **S&P 500** | ~500 | Active | Wikipedia |
| **Dow Jones** | ~30 | Active | Wikipedia |
| **Nasdaq 100** | ~100 | Active | Wikipedia |

## Quick Start

### Prerequisites

```bash
# Ensure you're in the project root
cd /Users/jakedugan/Projects/corp-speech-data-repo

# Install dependencies
uv sync

# Activate environment
source .venv/bin/activate
```

### Basic Usage

Extract S&P 500 constituents:

```bash
# Extract S&P 500 to CSV and Parquet
uv run python -m corpus_hydrator.cli.fetch index-constituents --index sp500
```

Extract all supported indices:

```bash
# Extract all indices
uv run python -m corpus_hydrator.cli.fetch index-constituents --index all
```

### Advanced Usage

```bash
# Extract with all options
uv run python -m corpus_hydrator.cli.fetch index-constituents \
  --index sp500 \
  --format csv parquet \
  --output-dir ./my_data \
  --force \
  --cache-ttl 3600 \
  --verbose
```

## Output Files

### Generated Files

For each index extraction, the system creates:

```
data/
├── sp500_constituents.csv              # CSV format
├── sp500_constituents.parquet          # Parquet format
└── sp500_manifest.json                 # Metadata manifest
```

### CSV Format Example

```csv
symbol,company_name,index_name,sector,industry,date_added,source_url
AAPL,Apple Inc.,S&P 500,Technology,Consumer Electronics,1982-11-30,https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
MSFT,Microsoft Corp.,S&P 500,Technology,Software,1994-06-01,https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
```

### Manifest File

```json
{
  "index_name": "S&P 500",
  "rows": 503,
  "schema_version": "1.0",
  "extracted_at": "2025-09-11T12:41:15.063277",
  "source_url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
  "sha256_csv": "d7913e9cacbbedf32f1e39879e02ead0ea829c0f0d1c7ffb7d78983cc75585c5",
  "sha256_parquet": "224cfc301bc67f76703e004cd743d3dd7e0ba7d3660b63e49ef42573e262db7e",
  "format": "index_constituents",
  "description": "Constituents for S&P 500"
}
```

## Testing

### Run All Tests

```bash
# Run the complete test suite
uv run python -m pytest packages/corpus-hydrator/tests/unit/adapters/index_constituents/ packages/corpus-hydrator/tests/contracts/ -v
```

### Test Specific Components

```bash
# Test configuration
uv run python -m pytest packages/corpus-hydrator/tests/unit/adapters/index_constituents/test_config.py -v

# Test data normalization
uv run python -m pytest packages/corpus-hydrator/tests/unit/adapters/index_constituents/test_normalize.py -v

# Test file writing
uv run python -m pytest packages/corpus-hydrator/tests/unit/adapters/index_constituents/test_writer.py -v

# Test contracts (ensures system reliability)
uv run python -m pytest packages/corpus-hydrator/tests/contracts/test_provider_contract.py -v
```

### Manual Testing

```bash
# Test with verbose output
uv run python -m corpus_hydrator.cli.fetch index-constituents --index sp500 --verbose

# Test caching (second run should be faster)
uv run python -m corpus_hydrator.cli.fetch index-constituents --index sp500

# Test different output formats
uv run python -m corpus_hydrator.cli.fetch index-constituents --index sp500 --format csv
uv run python -m corpus_hydrator.cli.fetch index-constituents --index sp500 --format parquet
```

## Key Files Explained

### Core Components

#### `providers/wikipedia.py`
**Purpose**: Main data provider that scrapes Wikipedia pages
**Key Features**:
- Extracts HTML content from Wikipedia
- Handles different table structures
- Implements rate limiting and caching
- Returns structured data for parsing

#### `parsers/html_table.py`
**Purpose**: Parses HTML tables into structured data
**Key Features**:
- Finds constituent tables on Wikipedia pages
- Extracts headers and data rows
- Handles various table formats
- Cleans and normalizes extracted data

#### `normalize.py`
**Purpose**: Standardizes raw data into consistent format
**Key Features**:
- Maps different column names to standard fields
- Converts data types appropriately
- Handles missing or malformed data
- Ensures data quality and consistency

#### `usecase.py`
**Purpose**: Orchestrates the entire extraction process
**Key Features**:
- Coordinates providers, parsers, and writers
- Handles errors gracefully
- Provides consistent interface
- Manages data flow between components

#### `writer.py`
**Purpose**: Generates output files with metadata
**Key Features**:
- Creates CSV and Parquet files
- Generates manifest files with SHA256 hashes
- Ensures deterministic output ordering
- Provides data integrity verification

### Configuration & Utils

#### `config.py`
**Purpose**: Defines index configurations and settings
**Contains**:
- URLs for each index
- Column mappings for different sources
- Table selectors and parsing rules
- Index metadata and descriptions

#### `utils/http.py`
**Purpose**: HTTP client with intelligent caching
**Features**:
- ETag-based caching for efficiency
- Configurable TTL settings
- Error handling and retries
- Rate limiting support

### Command Line Interface

#### `cli/fetch.py`
**Purpose**: Command-line interface for the system
**Commands**:
- `index-constituents`: Main extraction command
- Supports multiple indices, formats, and options
- Provides progress reporting and error handling

## Configuration Options

### CLI Options

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `--index` | Index to extract | Required | `--index sp500` |
| `--format` | Output formats | `csv parquet` | `--format csv` |
| `--output-dir` | Output directory | `./data` | `--output-dir ./output` |
| `--force` | Bypass cache | `false` | `--force` |
| `--cache-ttl` | Cache time-to-live (seconds) | `3600` | `--cache-ttl 7200` |
| `--verbose` | Detailed output | `false` | `--verbose` |

### Index Options

- `sp500` - S&P 500 constituents
- `dow` - Dow Jones Industrial Average
- `nasdaq100` - Nasdaq 100
- `all` - All supported indices

## Troubleshooting

### Common Issues

#### "No data extracted"
```bash
# Try with force refresh
uv run python -m corpus_hydrator.cli.fetch index-constituents --index sp500 --force --verbose
```

#### "Network timeout"
```bash
# Increase cache TTL to reduce requests
uv run python -m corpus_hydrator.cli.fetch index-constituents --index sp500 --cache-ttl 7200
```

#### "Permission denied"
```bash
# Ensure output directory exists and is writable
mkdir -p ./data
uv run python -m corpus_hydrator.cli.fetch index-constituents --index sp500 --output-dir ./data
```

### Debug Mode

```bash
# Enable verbose logging
uv run python -m corpus_hydrator.cli.fetch index-constituents --index sp500 --verbose

# Check generated files
ls -la data/
cat data/sp500_manifest.json
head -5 data/sp500_constituents.csv
```

## Performance & Reliability

### Caching
- **HTTP ETags**: Avoids re-downloading unchanged content
- **Configurable TTL**: Balance freshness vs. performance
- **Local storage**: Reduces network dependency

### Error Handling
- **Graceful failures**: Continues processing when possible
- **Detailed logging**: Easy troubleshooting
- **Fallback support**: Architecture ready for multiple providers

### Data Quality
- **SHA256 verification**: Ensures data integrity
- **Manifest metadata**: Complete audit trail
- **Deterministic output**: Consistent results across runs

## Future Extensions

### Ready-to-Extend Features

#### Add New Data Providers
```python
# Example: Add SEC EDGAR provider
class SECProvider(IndexProvider):
    def fetch_raw(self, index_key: str) -> Mapping[str, Any]:
        # SEC API logic here
        pass
```

#### Add New Indices
```python
# Add to config.py
INDEX_CONFIGS['russell2000'] = IndexConfig(
    name="Russell 2000",
    url="https://en.wikipedia.org/wiki/Russell_2000_Index",
    table_selector="#constituents",
    column_map={
        "Symbol": "symbol",
        "Company": "company_name",
        "Sector": "sector"
    }
)
```

#### Add New Output Formats
```python
# Extend writer.py
def write_jsonl(df: pd.DataFrame, output_path: Path) -> str:
    # JSONL export logic
    pass
```

## Support

### Getting Help

1. **Check the logs**: Use `--verbose` for detailed output
2. **Verify network**: Ensure internet connectivity
3. **Check permissions**: Ensure write access to output directory
4. **Run tests**: Verify system health with test suite

### Expected Results

For a successful extraction, you should see:
- Progress messages during extraction
- File creation confirmations
- Final summary with company counts
- No error messages in logs

## Integration with Downstream Systems

### CourtListener Query Enhancement

The generated company lists are used to:
1. **Prioritize searches** for company-related documents
2. **Filter results** to focus on relevant cases
3. **Improve query performance** with targeted searches

### NER Model Support

The constituent data helps NER systems by:
1. **Providing vocabulary** of company names
2. **Improving recognition accuracy** for financial entities
3. **Reducing false positives** in entity detection

### Usage in Queries

```python
# Example: Enhanced CourtListener query
companies = load_company_list('sp500_constituents.csv')
enhanced_query = create_company_focused_query(base_query, companies)
results = courtlistener.search(enhanced_query)
```

---

**Remember**: This system provides **valuable signal** for downstream processes. Perfect accuracy isn't required - the goal is to **significantly improve** the relevance and efficiency of legal document processing and company entity recognition!
