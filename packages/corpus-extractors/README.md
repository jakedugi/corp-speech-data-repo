# Corpus Extractors

Information extraction module for the corporate speech risk dataset. This module processes normalized legal documents to extract quotes, outcomes, cash amounts, and assign case-level values.

## Overview

The corpus-extractors module serves as the information extraction layer for the corpus pipeline, providing:

- **Quote Extraction**: Identifies and extracts quoted speech from legal documents
- **Speaker Attribution**: Links quotes to speakers using contextual analysis
- **Outcome Extraction**: Parses case dispositions, settlements, and financial penalties
- **Cash Amount Detection**: Specialized extraction of monetary amounts and penalties
- **Case Value Assignment**: Deterministic case-level monetary value assignment

## Installation

```bash
pip install -e .
```

## Quick Start

### Extract Quotes
```bash
# Extract quotes from normalized documents
python -m corpus_extractors.cli.extract quotes \
    --input data/courtlistener_normalized.jsonl \
    --output data/quotes.jsonl
```

### Extract Outcomes
```bash
# Extract outcomes from normalized documents
python -m corpus_extractors.cli.extract outcomes \
    --input data/courtlistener_normalized.jsonl \
    --output data/outcomes.jsonl
```

### Extract Cash Amounts
```bash
# Extract cash amounts from normalized documents
python -m corpus_extractors.cli.extract cash_amounts \
    --input data/courtlistener_normalized.jsonl \
    --output data/cash_amounts.jsonl
```

### Assign Case Values
```bash
# Assign case-level monetary values to quotes
python -m corpus_extractors.assign_case_values \
    --cash data/cash_amounts.jsonl \
    --outcomes data/outcomes.jsonl \
    --quotes data/quotes.jsonl \
    --preferred_outcome stipulated_judgment \
    --out data/quotes_with_case_values.jsonl
```

### Programmatic Usage
```python
from corpus_extractors import QuoteExtractor, assign_case_values

# Extract quotes
extractor = QuoteExtractor()
quotes = extractor.extract_from_documents(documents)

# Assign case values
enriched_quotes = assign_case_values(
    quotes=quotes,
    outcomes=outcomes,
    cash_amounts=cash_amounts,
    preferred_outcome_type='stipulated_judgment'
)

# Extract from document text
quotes = extractor.extract_quotes(raw_text)
for quote in quotes:
    print(f"{quote.speaker}: {quote.quote}")
```

### Assign Case Values
```bash
# Assign deterministic case-level monetary values to quotes
python -m corpus_extractors.assign_case_values \
    --cash data/cash_amounts.jsonl \
    --outcomes data/outcomes.jsonl \
    --quotes data/quotes.jsonl \
    --preferred_outcome stipulated_judgment \
    --out data/quotes_with_case_values.jsonl \
    --also_injunctive_relief true
```

## Module Structure

```
corpus_extractors/
├── cli/
│   └── extract.py              # Main CLI interface
├── extraction_pipeline/        # Core extraction logic
│   ├── quote_extractor.py      # Quote detection and attribution
│   ├── extract_quotes.py       # Quote extraction functions
│   ├── extract_outcomes.py     # Outcome extraction
│   ├── extract_cash_amounts_stage1.py # Cash amount detection
│   ├── first_pass.py          # Initial extraction pass
│   ├── rerank.py              # Semantic reranking
│   ├── attribution.py         # Speaker attribution
│   └── final_pass_filter.py   # Final filtering
├── case_assignment/           # Case value assignment
│   ├── assign_case_values.py  # Case-level value assignment
│   └── validate_case_values.py # Assignment validation
├── infrastructure/            # Base classes and utilities
│   ├── base_extractor.py      # Base extraction classes
│   ├── case_outcome_imputer.py # Outcome imputation logic
│   ├── court_provenance.py    # Court metadata handling
│   ├── process_documents.py   # Document processing utilities
│   └── registry.py            # Component registry
├── position_features/         # Positional feature extraction
├── tests/                     # Comprehensive test suite
├── configs/                   # Configuration files
├── fixtures/                  # Test fixtures
└── scripts/                   # Utility scripts
```

## Case Value Assignment

### Business Rules

The case value assignment follows deterministic priority logic:

1. **Priority 1**: Non-zero `preferred_outcome_type` amounts (largest wins)
2. **Priority 2**: Cash amounts with highest `feature_votes > 0` (largest amount wins)
3. **Fallback**: "N/A" if no valid sources

### Output Schema

```json
{
  "case_id": "1:13-cv-00002",
  "doc_id": "1:13-cv-00002_dcd_entry_2930836",
  "quote_text": "Required information",
  "speaker": "it",
  "assigned_case_value": 455000.0,
  "value_source": "outcome_metadata.stipulated_judgment",
  "preferred_outcome_type": "stipulated_judgment",
  "source_outcome_doc_ids": ["1:13-cv-00002_dcd_entry_2930836"],
  "source_cash_doc_ids": []
}
```

## Extraction Capabilities

### Quote Extraction
- **Pattern Recognition**: Multiple quote pattern detection (double quotes, guillemets, etc.)
- **Speaker Attribution**: Contextual speaker identification
- **Span Detection**: Precise character-level span identification
- **Confidence Scoring**: Quality assessment for each extracted quote

### Outcome Extraction
- **Disposition Parsing**: Win/loss determination from court language
- **Settlement Detection**: Identification of settlement agreements
- **Cash Amount Extraction**: Monetary penalty and settlement amount detection
- **Metadata Collection**: Judge names, court information, case numbers

### Quality Assurance
- **Confidence Thresholds**: Configurable quality thresholds
- **Duplicate Removal**: Automatic detection and removal of duplicates
- **Validation**: Schema validation and consistency checks
- **Evaluation Metrics**: Precision, recall, and F1-score calculation

## Configuration

All extraction behavior is controlled via `configs/quotes.yaml`. The QuoteExtractor automatically loads this as defaults, and you can override any setting programmatically.

### Quote Extraction Config

Key configuration sections in `configs/quotes.yaml`:

```yaml
# NLP model settings
nlp:
  spacy_model: "en_core_web_sm"
  role_keywords: ["CEO", "CFO", "President", "Officer", ...]

# Extraction parameters
extraction:
  keywords: ["regulation", "policy", "statement", ...]
  company_aliases: ["company", "corporation", "inc", ...]
  min_quote_length: 10
  max_quote_length: 10000

# Semantic reranking
reranking:
  enabled: true
  model: "all-mpnet-base-v2"
  threshold: 0.55
  seed_quotes:
    - "The company stated that"
    - "According to the policy"
    ...
```

See `configs/quotes.yaml` for complete configuration options.

### Outcome Extraction Config
```yaml
extraction:
  outcome_patterns:
    - "(?:granted|denied|dismissed)"
  categories:
    win: ["granted", "affirmed"]
    loss: ["denied", "dismissed"]
    settlement: ["settlement", "consent"]

labeling:
  min_confidence:
    win: 0.8
    loss: 0.8
    settlement: 0.7
```

## Data Formats

### Input Documents

Input should be normalized JSONL documents from `corpus-cleaner`:

```json
{
  "schema_version": "1.0",
  "doc_id": "1:13-cv-00002_dcd_entry_2930827",
  "source_uri": "https://www.courtlistener.com/...",
  "raw_text": "Justice Roberts stated that 'the precedent is clear'...",
  "meta": {"case_id": "1:13-cv-00002_dcd", "source_type": "entry"}
}
```

### Output Quotes

Extracted quotes with attribution and confidence scores:

```json
{
  "text": "the precedent is clear",
  "speaker": "Justice Roberts",
  "score": 0.95,
  "context": "...Justice Roberts stated that 'the precedent is clear' in his opinion...",
  "urls": [],
  "doc_id": "1:13-cv-00002_dcd_entry_2930827"
}
```

**Note**: Types are defined in `corpus-types` module following SSOT principles.

### Output Outcomes
```json
{
  "case_id": "case_001",
  "doc_id": "doc_001",
  "label": "win",
  "confidence": 0.92,
  "cash_amount": null,
  "meta": {"judge": "Justice Roberts"}
}
```

## Advanced Features

### Semantic Reranking
Uses sentence transformers to improve quote quality by reranking based on semantic relevance.

### Cash Amount Processing
Specialized patterns for detecting and normalizing monetary amounts in legal text.

### Cross-Reference Resolution
Links related quotes and outcomes across multiple documents in the same case.

## Development

See [DEVELOPMENT.md](./DEVELOPMENT.md) for detailed development guide, architecture documentation, and future roadmap.

### Running Tests

Unit tests:
```bash
pytest tests/unit/ -v
```

Integration tests (end-to-end with real data):
```bash
pytest tests/integration/ -v
```

### Architecture

The quote extraction pipeline follows SOC (Separation of Concerns) principles:

1. **FirstPassExtractor**: Pattern-based extraction
2. **Attributor**: Multi-sieve speaker attribution with configurable NLP
3. **SemanticReranker**: Quality filtering via sentence transformers
4. **QuoteExtractor**: Orchestrates the full pipeline with config management

### Configuration Philosophy

- **Defaults in YAML**: All defaults in `configs/quotes.yaml`
- **Runtime Override**: User config deep-merges over defaults
- **Non-Breaking**: All knobs configurable, APIs backward compatible

## Performance

- **Batch Processing**: Efficient processing of large document collections
- **Memory Management**: Streaming processing for memory efficiency
- **Parallel Processing**: Multi-worker support for CPU-intensive tasks
- **Caching**: Intermediate result caching for iterative development

## Integration

The corpus-extractors module integrates with:
- **corpus-cleaner**: Consumes normalized documents (input)
- **corpus-features**: Provides quotes for feature extraction (output)
- **corpus-types**: Uses document and quote schemas (SSOT for all types)

## Contributing

1. Follow the established patterns for new extractors
2. Include comprehensive test coverage
3. Update configuration schemas
4. Document extraction logic and edge cases
5. Ensure backwards compatibility

## License

This module is part of the corpus project and follows the same license terms.
