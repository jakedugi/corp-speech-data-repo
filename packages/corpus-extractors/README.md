# Corpus Extractors

Quote and outcome extraction module for the corporate speech risk dataset. This module processes normalized legal documents to extract quoted speech, case outcomes, and associated metadata.

## Overview

The corpus-extractors module serves as the information extraction layer in the corpus pipeline, providing:

- **Quote Extraction**: Identifies and extracts quoted speech from legal documents
- **Speaker Attribution**: Links quotes to speakers using contextual analysis
- **Outcome Extraction**: Parses case dispositions, settlements, and financial penalties
- **Cash Amount Detection**: Specialized extraction of monetary amounts and penalties
- **Quality Assurance**: Confidence scoring and validation of extracted information

## Installation

```bash
pip install -e .
```

## Quick Start

### Extract Quotes
```bash
# Extract quotes from normalized documents
corpus-extract-quotes --input data/docs.norm.jsonl --output data/quotes.jsonl --config configs/quotes.yaml
```

### Extract Outcomes
```bash
# Extract case outcomes and labels
corpus-extract-outcomes --input data/docs.norm.jsonl --output data/outcomes.jsonl --config configs/outcomes.yaml
```

## Module Structure

```
corpus_extractors/
├── cli/
│   └── extract.py              # Main CLI interface
├── quote_extractor.py          # Quote detection and attribution
├── case_outcome_imputer.py     # Outcome extraction logic
├── extract_cash_amounts_stage1.py # Cash amount detection
├── final_evaluate.py           # Quality evaluation
├── first_pass.py              # Initial extraction pass
├── attribution.py             # Speaker attribution
├── rerank.py                  # Semantic reranking
├── final_pass_filter.py       # Final filtering
├── base_extractor.py          # Base extraction classes
├── tests/                     # Comprehensive test suite
├── configs/                   # Configuration files
├── fixtures/                  # Test fixtures
└── misc/                      # Utility scripts
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

### Quote Extraction Config
```yaml
extraction:
  quote_patterns:
    - '"([^"]*)"'
    - '«([^»]*)»'
  min_quote_length: 10
  attribution:
    enabled: true
    max_speaker_distance: 500

reranking:
  enabled: true
  model: "sentence-transformers/all-MiniLM-L6-v2"
  threshold: 0.8
```

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
```json
{
  "doc_id": "doc_001",
  "raw_text": "Justice Roberts stated that 'the precedent is clear'...",
  "meta": {"court": "scotus", "docket": "22-123"}
}
```

### Output Quotes
```json
{
  "quote_id": "q_001",
  "doc_id": "doc_001",
  "span": {"start": 25, "end": 46},
  "text": "the precedent is clear",
  "speaker": "Justice Roberts",
  "confidence": 0.95
}
```

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

### Running Tests
```bash
pytest tests/
```

### Adding New Extractors
1. Create extractor class inheriting from BaseExtractor
2. Implement extraction methods
3. Add configuration schema
4. Create test fixtures
5. Update CLI interface

## Performance

- **Batch Processing**: Efficient processing of large document collections
- **Memory Management**: Streaming processing for memory efficiency
- **Parallel Processing**: Multi-worker support for CPU-intensive tasks
- **Caching**: Intermediate result caching for iterative development

## Integration

The corpus-extractors module integrates with:
- **corpus-cleaner**: Consumes normalized documents
- **corpus-features**: Provides quotes for feature extraction
- **corpus-types**: Uses document and quote schemas

## Contributing

1. Follow the established patterns for new extractors
2. Include comprehensive test coverage
3. Update configuration schemas
4. Document extraction logic and edge cases
5. Ensure backwards compatibility

## License

This module is part of the corpus project and follows the same license terms.
