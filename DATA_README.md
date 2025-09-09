# Corporate Speech Data Pipeline

This repository contains the data processing pipeline for extracting corporate speech legal risk examples from court documents, RSS feeds, and other sources.

## What This Does

The data pipeline transforms raw legal documents into structured datasets:

1. **Fetch**: Collects documents from CourtListener API, RSS feeds, and web scraping
2. **Normalize**: Cleans and standardizes text formatting
3. **Extract**: Identifies quotes, speakers, and case outcomes
4. **Validate**: Ensures data quality and schema compliance
5. **Manifest**: Generates reproducible fingerprints and metadata

## Quick Start

```bash
# Run the full pipeline
make demo_e2e

# Or run individual steps
make clean fetch normalize extract validate manifest
```

## Expected Output

After running `make demo_e2e`, you'll get:

```
data/
├── docs.raw.jsonl      # Raw documents from sources
├── docs.norm.jsonl     # Normalized/cleaned documents
├── quotes.jsonl        # Extracted quotes with speakers
├── outcomes.jsonl      # Case outcomes and labels
├── manifest.json       # Metadata, versions, fingerprints
└── RUN.md             # Execution log
```

## Sample Data

### Raw Document (docs.raw.jsonl)
```json
{
  "schema_version": "1.0",
  "doc_id": "doc_001",
  "source_uri": "https://www.courtlistener.com/opinion/12345/",
  "raw_text": "Justice Roberts stated that 'the precedent is clear'...",
  "meta": {"court": "scotus", "docket": "22-123"},
  "provenance": {...}
}
```

### Normalized Document (docs.norm.jsonl)
```json
{
  "schema_version": "1.0",
  "doc_id": "doc_001",
  "raw_text": "Justice Roberts stated that 'the precedent is clear'...",
  "normalized": true,
  "meta": {"court": "scotus", "docket": "22-123"}
}
```

### Extracted Quote (quotes.jsonl)
```json
{
  "schema_version": "1.0",
  "quote_id": "q_001_25_46",
  "doc_id": "doc_001",
  "text": "the precedent is clear",
  "speaker": "Justice Roberts",
  "score": 0.95,
  "span": {"start": 25, "end": 46}
}
```

### Case Outcome (outcomes.jsonl)
```json
{
  "schema_version": "1.0",
  "case_id": "case_001",
  "label": "win",
  "label_source": "manual",
  "date": "2023-06-15",
  "meta": {"court": "scotus", "docket": "22-123"}
}
```

## Configuration

### Query Configuration (configs/query.small.yaml)
```yaml
sources:
  - courtlistener

courtlistener:
  date_range:
    start: "2023-01-01"
    end: "2023-06-30"
  courts: ["scotus"]
  keywords: ["corporation", "securities"]
  max_results: 5
```

## Data Contracts

All data follows the schemas defined in `corpus-types`:

- **Doc**: Raw and normalized documents
- **Quote**: Extracted quotes with metadata
- **Outcome**: Case outcomes and labels

Validate any output file:
```bash
corpus-validate jsonl Doc data/docs.norm.jsonl
corpus-validate jsonl Quote data/quotes.jsonl
corpus-validate jsonl Outcome data/outcomes.jsonl
```

## Deterministic Outputs

The pipeline produces **deterministic outputs**:
- Same inputs → same outputs
- **blake3 fingerprints** in manifest.json should be identical across runs
- **Stable IDs**: doc_id, quote_id, case_id remain consistent

## Development

### Testing
```bash
# Run all tests
pytest

# Run e2e tests
pytest tests/test_e2e_data.py

# Run specific module tests
pytest corpus_api/api_tests/
pytest corpus_cleaner/tests/
pytest corpus_extractors/tests/
pytest corpus_types/tests/
```

### Adding Fixtures
For offline testing, add small fixture files to `corpus_types/fixtures/`:
- `docs.raw.small.jsonl` - Raw documents
- `quotes.small.jsonl` - Expected quotes
- `outcomes.small.jsonl` - Expected outcomes

### CI/CD
The pipeline runs in CI with:
- Multi-Python version testing (3.8-3.11)
- Code quality checks (black, isort, mypy, flake8)
- Coverage reporting
- Offline fixture testing

## Hand-off to Modeling

The Data pipeline produces clean, validated datasets that Modeling can consume:

- **docs.norm.jsonl**: Clean text for feature extraction
- **quotes.jsonl**: Structured quotes with speakers
- **outcomes.jsonl**: Labeled case outcomes
- **manifest.json**: Complete provenance and reproducibility info

No feature engineering happens in Data - that's the Modeling repo's responsibility.

## Troubleshooting

### Common Issues

**Import Errors**: Make sure all modules are installed:
```bash
pip install -e corpus_types/
pip install -e corpus_api/
pip install -e corpus_cleaner/
pip install -e corpus_extractors/
```

**Schema Validation Fails**: Check the first few records:
```bash
head -5 data/docs.norm.jsonl | corpus-validate jsonl Doc -
```

**Non-deterministic Outputs**: Check for:
- Random seeds not set
- Dictionary iteration order
- Floating-point precision
- File timestamps in outputs

**Missing CLI Commands**: Ensure entry points are installed:
```bash
corpus-fetch --help
corpus-clean --help
corpus-extract-quotes --help
corpus-validate --help
```

## Module Status

| Module | Status | Description |
|--------|--------|-------------|
| corpus-types | ✅ Production | Data schemas and validation |
| corpus-api | ⚠️ Needs fixes | Import path issues resolved, needs testing |
| corpus-cleaner | ✅ Working | Basic normalization implemented |
| corpus-extractors | ⚠️ Needs fixes | Advanced but needs import fixes |
| Data Orchestrator | ✅ Working | Makefile and scripts ready |

## Next Steps

1. **Fix remaining import issues** in corpus-extractors
2. **Implement full offset mapping** in cleaner
3. **Add deterministic ID generation** in extractors
4. **Add comprehensive validation tests**
5. **Set up Docker containerization**
