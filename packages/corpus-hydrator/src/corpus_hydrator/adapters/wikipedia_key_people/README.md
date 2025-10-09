# Wikipedia Key People Extraction System

## Overview

The **Wikipedia Key People Extraction System** is a sophisticated, production-ready component that extracts comprehensive executive and key personnel information from Wikipedia company pages. This system provides structured data about company leadership, board members, and key roles to support downstream analysis, Named Entity Recognition (NER) models, and corporate research workflows.

## Purpose & Importance

### Why This Matters

This system serves as a **comprehensive data enrichment layer** that significantly enhances multiple downstream processes:

1. **Corporate Research Enhancement**: Provides detailed executive and board member information for company analysis
2. **NER Model Training**: Supplies high-quality labeled data for training executive recognition models
3. **Risk Assessment Support**: Enables analysis of corporate governance and leadership changes
4. **Regulatory Compliance**: Supports monitoring of key personnel for compliance and due diligence
5. **Investment Analysis**: Provides data for analyzing leadership impact on company performance

### Accuracy Expectations

**Important**: This system provides **high-quality, structured data** with the following quality standards:

- **Data Completeness**: Extracts comprehensive information when available
- **Normalization Quality**: Applies advanced cleaning and standardization
- **Source Reliability**: Uses authoritative Wikipedia data with Wikidata integration
- **Structured Output**: Produces relational data with proper relationships
- **Audit Trail**: Includes confidence scores and extraction metadata

## Architecture Overview

The system follows **Clean Architecture** principles with clear separation of concerns and modular design:

```
Wikipedia Key People System
├── providers/          # Data Sources & Strategies
│   ├── wikidata.py        # Wikidata structured API
│   └── base.py           # Provider interfaces
├── parsers/           # Data Extraction
│   └── base.py           # Base parsing classes
├── normalize.py       # Data Standardization & Cleaning
├── usecase.py         # Business Logic & Orchestration
├── writer.py          # Data Output & Serialization
├── config.py          # Configuration Management
├── utils/             # Enhanced HTTP & Utilities
│   ├── http.py           # Advanced HTTP client with caching
│   ├── enums.py          # Type-safe enumerations
│   └── logging_utils.py  # Structured logging
├── cli/commands.py    # Command Line Interface
└── Enhanced testing   # Contract tests & validation
```

## Data Flow

1. **Index Processing** → Extract company URLs from market index pages
2. **Page Fetching** → Retrieve individual company Wikipedia pages with caching
3. **Multi-Strategy Extraction** → Apply infobox, section, table, and list parsing
4. **Wikidata Integration** → Enhance with structured Wikidata relationships
5. **Advanced Normalization** → Clean, deduplicate, and standardize data
6. **Relationship Building** → Create company-person-role connections
7. **Quality Assurance** → Validate and score data quality
8. **Output Generation** → Produce CSV, Parquet, and manifest files

## Supported Features

| Feature | Status | Description |
|---------|--------|-------------|
| **S&P 500** | Active | Full extraction with 500+ companies |
| **Dow Jones** | Active | Complete extraction with 30 companies |
| **NASDAQ-100** | Active | Comprehensive extraction with 100 companies |
| **Multi-Strategy Parsing** | Active | Infobox, sections, tables, lists |
| **Wikidata Integration** | Active | Structured data enhancement |
| **Advanced Normalization** | Active | Unicode NFC, deduplication, controlled vocab |
| **HTTP Caching** | Active | ETag, Last-Modified, revision tracking |
| **CLI Interface** | Active | Full command-line support |
| **Structured Logging** | Active | JSON logging with metrics |
| **Contract Testing** | Active | Comprehensive test coverage |

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

Extract S&P 500 key people data:

```bash
# Extract S&P 500 with production-ready normalized output
uv run python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized --index sp500 --verbose
```

Extract Dow Jones with enhanced features:

```bash
# Extract Dow Jones with all enhancements
uv run python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized --index dow --workers 2 --verbose
```

### Advanced Usage

```bash
# Extract with comprehensive options
uv run python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized \
  --index sp500 \
  --workers 4 \
  --force-refresh \
  --cache-dir ~/.cache/wikipedia_key_people \
  --requests-per-second 0.75 \
  --timeout 15 \
  --verbose

# Dry run for testing
uv run python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized \
  --index dow \
  --max-companies 5 \
  --dry-run \
  --verbose
```

## Output Files

### Generated Files

For each index extraction, the system creates:

```
data/
├── {index}_key_people_companies.csv         # Company metadata
├── {index}_key_people_companies.parquet     # Company data (Parquet)
├── {index}_key_people_people.csv            # Person information
├── {index}_key_people_people.parquet        # Person data (Parquet)
├── {index}_key_people_roles.csv             # Role definitions
├── {index}_key_people_roles.parquet         # Role data (Parquet)
├── {index}_key_people_appointments.csv      # Company-person-role relationships
├── {index}_key_people_appointments.parquet  # Appointment data (Parquet)
└── {index}_key_people_manifest.json         # Comprehensive metadata
```

### Companies CSV Format

```csv
company_id,company_name,ticker,wikipedia_url,wikidata_qid,index_name,processed_at
AAPL_dow,Apple Inc.,AAPL,https://en.wikipedia.org/wiki/Apple_Inc.,Q312,Q312,dow,2025-09-11T18:31:04.749
```

### People CSV Format

```csv
person_id,full_name,normalized_name,wikidata_qid,created_at
tim_cook_001,Timothy Cook,Tim Cook,,2025-09-11T18:31:04.698
```

### Roles CSV Format

```csv
role_id,role_canon,role_raw
ceo_001,Chief Executive Officer,CEO
cfo_001,Chief Financial Officer,CFO
```

### Appointments CSV Format

```csv
company_id,person_id,role_id,start_date,end_date,source_url,source_revision_id,extraction_strategy,confidence_score,extracted_at
AAPL_dow,tim_cook_001,ceo_001,,https://en.wikipedia.org/wiki/Apple_Inc.,,wikipedia_infobox,0.95,2025-09-11T18:31:04.698
```

### Manifest File

```json
{
  "schema_version": "2.0.0",
  "dataset_name": "dow_key_people",
  "extraction_timestamp": "2025-09-11T18:31:04.758",
  "row_counts": {
    "companies": 3,
    "people": 12,
    "roles": 6,
    "appointments": 12
  },
  "file_hashes": {
    "companies_csv": "78110554...",
    "people_csv": "2fdab52a...",
    "roles_csv": "7d4103d9...",
    "appointments_csv": "a5dde78c..."
  },
  "source_metadata": {
    "provider_order": ["wikipedia"],
    "extraction_parameters": {
      "include_parquet": true,
      "deterministic_sorting": true
    },
    "data_quality_metrics": {
      "total_entities": 33,
      "data_completeness": "high"
    }
  },
  "governance": {
    "created_by": "wikipedia-key-people-scraper",
    "license": "CC-BY-SA 4.0 (inherited from Wikipedia)",
    "attribution_required": true,
    "contact": "jake@jakedugan.com"
  }
}
```

## Testing

### Run All Tests

```bash
# Run the complete test suite
uv run python -m pytest packages/corpus-hydrator/tests/unit/adapters/wikipedia_key_people/ packages/corpus-hydrator/tests/contracts/test_wikipedia_key_people_contract.py -v
```

### Test Specific Components

```bash
# Test configuration
uv run python -m pytest packages/corpus-hydrator/tests/unit/adapters/wikipedia_key_people/test_config.py -v

# Test normalization
uv run python -m pytest packages/corpus-hydrator/tests/unit/adapters/wikipedia_key_people/test_normalize.py -v

# Test data writing
uv run python -m pytest packages/corpus-hydrator/tests/unit/adapters/wikipedia_key_people/test_writer.py -v

# Test CLI commands
uv run python -m pytest packages/corpus-hydrator/tests/unit/adapters/wikipedia_key_people/test_cli.py -v

# Test contract compliance
uv run python -m pytest packages/corpus-hydrator/tests/contracts/test_wikipedia_key_people_contract.py -v
```

### Manual Testing

```bash
# Test with verbose output
uv run python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized --index dow --max-companies 3 --verbose

# Test Wikidata integration
uv run python -m pytest packages/corpus-hydrator/tests/unit/adapters/wikipedia_key_people/test_wikidata_provider.py -v

# Test normalization quality
uv run python -c "
from corpus_hydrator.adapters.wikipedia_key_people.normalize import WikipediaKeyPeopleNormalizer
n = WikipediaKeyPeopleNormalizer()
test_name = 'José María González (CEO)'
normalized = n.normalize_name_unicode(test_name)
print(f'Original: {test_name}')
print(f'Normalized: {normalized}')
"
```

## Key Files Explained

### Core Components

#### `config.py`
**Purpose**: Configuration management and validation
**Key Features**:
- Index-specific configurations (S&P 500, Dow, NASDAQ-100)
- Scraping behavior settings (rate limits, timeouts, retries)
- Content extraction patterns and keywords
- Validation functions for configuration integrity

#### `usecase.py`
**Purpose**: Business logic orchestration and workflow management
**Key Features**:
- Coordinates extraction from index pages to individual companies
- Manages parallel processing with worker pools
- Handles error recovery and retry logic
- Provides clean API for different extraction scenarios

#### `writer.py`
**Purpose**: Data output generation with integrity verification
**Key Features**:
- Deterministic CSV and Parquet file generation
- SHA256 hash calculation for data integrity
- Comprehensive manifest creation with governance metadata
- Support for both legacy and normalized data formats

#### `normalize.py`
**Purpose**: Advanced data cleaning and standardization
**Key Features**:
- Unicode NFC normalization for international names
- Controlled vocabulary mapping for job titles
- Advanced deduplication with conflict resolution
- Batch processing with configurable options

### Advanced Components

#### `core/enhanced_scraper.py`
**Purpose**: Multi-strategy extraction engine
**Key Features**:
- Infobox parsing for structured data
- Section-based extraction for various layouts
- Table parsing for tabular executive data
- List processing for bullet-point information
- Confidence scoring and method selection

#### `providers/wikidata.py`
**Purpose**: Structured data enhancement via Wikidata API
**Key Features**:
- SPARQL query construction for executive data
- QID resolution from Wikipedia URLs
- Structured relationship extraction
- High-confidence structured data integration

#### `utils/http.py`
**Purpose**: Production-ready HTTP client with advanced features
**Key Features**:
- ETag and Last-Modified caching
- Automatic redirect handling
- Revision ID extraction from Wikipedia
- Rate limiting with jitter
- Comprehensive error handling

#### `utils/enums.py`
**Purpose**: Type-safe enumerations and constants
**Key Features**:
- Index type definitions (SP500, DOW, NASDAQ100)
- Extraction method enumerations
- Provider priority ordering
- Controlled vocabulary constants

### Command Line Interface

#### `cli/commands.py`
**Purpose**: User-friendly command-line interface
**Commands**:
- `scrape-index-normalized`: Production-ready extraction
- Support for all advanced options (workers, caching, dry-run)
- Progress reporting and error handling
- Structured output formatting

## Configuration Options

### CLI Options

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `--index` | Target index | Required | `--index sp500` |
| `--output-dir` | Output directory | `./data` | `--output-dir ./output` |
| `--max-companies` | Company limit | None | `--max-companies 50` |
| `--workers` | Parallel workers | 1 | `--workers 4` |
| `--force-refresh` | Skip cache | false | `--force-refresh` |
| `--clear-cache` | Clear HTTP cache | false | `--clear-cache` |
| `--dry-run` | Preview mode | false | `--dry-run` |
| `--verbose` | Detailed logging | false | `--verbose` |
| `--cache-dir` | Cache directory | ~/.cache | `--cache-dir ./cache` |
| `--requests-per-second` | Rate limit | 0.75 | `--requests-per-second 1.0` |
| `--timeout` | Request timeout | 15 | `--timeout 30` |

### Index Options

- `sp500` - S&P 500 (503 companies, comprehensive extraction)
- `dow` - Dow Jones Industrial Average (30 companies, detailed extraction)
- `nasdaq100` - NASDAQ-100 (100 companies, focused extraction)

### Advanced Configuration

```python
from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPeopleConfig

# Production configuration
config = WikipediaKeyPeopleConfig(
    enabled_indices=["sp500", "dow", "nasdaq100"],
    scraping=WikipediaScrapingConfig(
        wikipedia_rate_limit=0.75,  # Respect Wikipedia limits
        max_companies=None,         # Process all companies
        max_people_per_company=100, # Comprehensive extraction
        request_timeout=15,
        max_retries=5
    ),
    content=WikipediaContentConfig(
        # Enhanced role detection
        role_keywords=[
            "key people", "leadership", "executives", "management",
            "board", "directors", "officers", "corporate governance"
        ]
    )
)
```

## Troubleshooting

### Common Issues

#### "Extraction failed: name 'WikipediaKeyPeopleNormalizer' is not defined"
```bash
# Fix: Import error in usecase.py
# This has been resolved in the latest version
```

#### "HTTP 429 Too Many Requests"
```bash
# Solution: Reduce request rate
uv run python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized \
  --index sp500 \
  --requests-per-second 0.5 \
  --timeout 20
```

#### "Low data quality for certain companies"
```bash
# Expected: Some companies have minimal Wikipedia presence
# Solution: Check company Wikipedia page quality manually
# This is normal - not all companies have comprehensive executive data
```

#### "Unicode encoding errors"
```bash
# Solution: The system handles Unicode properly with NFC normalization
# If issues persist, check system locale settings
```

### Debug Mode

```bash
# Enable comprehensive logging
uv run python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized \
  --index dow \
  --max-companies 3 \
  --verbose \
  --force-refresh

# Check generated files
ls -la data/dow_key_people_*.csv
cat data/dow_key_people_manifest.json | jq '.row_counts'

# Test normalization quality
python3 -c "
from corpus_hydrator.adapters.wikipedia_key_people.normalize import WikipediaKeyPeopleNormalizer
n = WikipediaKeyPeopleNormalizer()
print('Unicode test:', n.normalize_name_unicode('José María (CEO)'))
print('Title test:', n.normalize_title_controlled_vocabulary('CEO'))
"
```

### Performance Issues

#### "Extraction taking too long"
```bash
# Solution: Use parallel processing
uv run python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized \
  --index sp500 \
  --workers 4 \
  --requests-per-second 1.0
```

#### "Memory usage high"
```bash
# Solution: Process in smaller batches
uv run python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized \
  --index sp500 \
  --max-companies 100 \
  --workers 2
```

## Performance & Reliability

### Caching Strategy
- **ETag Support**: Avoids re-downloading unchanged Wikipedia pages
- **Last-Modified Headers**: Intelligent conditional requests
- **Revision Tracking**: Detects page updates automatically
- **Configurable TTL**: Balance freshness vs. performance

### Error Handling
- **Graceful Degradation**: Continues processing when individual companies fail
- **Retry Logic**: Automatic retries with exponential backoff
- **Detailed Logging**: Comprehensive error reporting and debugging
- **Recovery Points**: Can resume interrupted extractions

### Data Quality
- **Multi-Strategy Parsing**: Fallback methods ensure maximum coverage
- **Confidence Scoring**: Quality assessment for all extracted data
- **Validation Pipeline**: Multiple validation stages ensure accuracy
- **Audit Trail**: Complete extraction metadata and source tracking

### Scalability
- **Parallel Processing**: Configurable worker pools for concurrent extraction
- **Memory Efficient**: Streaming processing for large datasets
- **Rate Limiting**: Polite extraction respecting source limits
- **Resource Monitoring**: Built-in performance metrics

## Future Extensions

### Ready-to-Extend Features

#### Add New Data Providers
```python
# Example: Add SEC EDGAR provider
class SECProvider:
    def fetch_company_executives(self, ticker: str) -> List[Dict]:
        # SEC API logic for executive compensation data
        # Extract from DEF 14A filings
        pass
```

#### Add New Extraction Strategies
```python
# Example: Add LLM-based extraction
class LLMExtractor:
    def extract_from_text(self, text: str) -> List[Dict]:
        # Use language models for complex text parsing
        # Handle edge cases that rule-based systems miss
        pass
```

#### Add New Output Formats
```python
# Example: Add database export
def write_to_database(data: Dict, connection_string: str):
    # Direct database insertion
    # Support for PostgreSQL, MySQL, etc.
    pass
```

#### Add Real-time Monitoring
```python
# Example: Add Prometheus metrics
class MetricsCollector:
    def record_extraction_metrics(self, company: str, duration: float, success: bool):
        # Export metrics to monitoring systems
        # Track performance and reliability
        pass
```

## Support

### Getting Help

1. **Check the logs**: Use `--verbose` for detailed extraction information
2. **Verify network**: Ensure stable internet connectivity for Wikipedia access
3. **Check permissions**: Ensure write access to output and cache directories
4. **Run tests**: Use the test suite to verify system health
5. **Check data quality**: Manually verify Wikipedia page content for problematic companies

### Expected Results

For a successful extraction, you should see:
- **Progress indicators** during company processing
- **Detailed logs** showing extraction strategies used
- **File creation confirmations** for all output formats
- **Quality metrics** in the manifest file
- **Structured data** in all CSV and Parquet files
- **No critical errors** in the final summary

### Data Quality Expectations

- **S&P 500**: 70-80% companies with extractable executive data
- **Dow Jones**: 80-90% success rate due to higher profile companies
- **NASDAQ-100**: 75-85% success rate with tech-focused companies
- **Overall Quality**: High-confidence normalized data with proper relationships

## 🎯 Integration with Downstream Systems

### NER Model Enhancement

The structured executive data significantly improves NER model performance:

```python
# Example: Enhanced NER training
executive_data = load_executive_dataset('sp500_key_people_appointments.csv')
person_entities = extract_person_entities(executive_data)
role_labels = extract_role_labels(executive_data)

# Train NER model with high-quality labeled data
ner_model.train(person_entities, role_labels)
```

### Corporate Research Workflows

The normalized data supports comprehensive corporate analysis:

```python
# Example: Leadership change analysis
leadership_data = load_appointments_data('sp500_key_people_appointments.csv')
changes = analyze_leadership_changes(leadership_data, date_range)

# Generate executive transition reports
reports.generate_executive_transition_report(changes)
```

---

**Remember**: This system provides **authoritative, structured executive data** that serves as a foundation for corporate analysis, NER model training, and compliance workflows. The multi-strategy approach and advanced normalization ensure **high-quality, reliable data** for production use!