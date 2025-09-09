# corpus-types

Authoritative **schemas**, **ID rules**, and a **validator CLI** for the corpus pipeline.

## Overview

This package provides:
- **Pydantic models** for data contracts (Doc, Quote, Outcome, etc.)
- **Deterministic ID generation** functions with BLAKE3 hashing
- **JSON Schema validation** and export utilities
- **CLI tools** for data validation (`corpus-validate`)

## Installation

```bash
pip install -e ".[dev]"
```

## Quick Start

### Generate IDs

```python
from corpus_types.ids.generate import doc_id, quote_id, case_id

# Generate document ID
doc_id_val = doc_id("https://example.com/case1", "2024-01-01T00:00:00Z", "N.D.Cal")
# Result: "doc_AAAAAAAAAAAAAAAAAAAAAA"

# Generate quote ID
quote_id_val = quote_id(doc_id_val, 10, 20, "we did nothing wrong.")
# Result: "q_BBBBBBBBBBBBBBBBBBBBB"

# Generate case ID
case_id_val = case_id("N.D.Cal", "1:24-cv-00001")
# Result: "case_CCCCCCCCCCCCCCCCCCCCC"
```

### Validate Data

```python
from corpus_types.schemas.models import Doc, Quote, Outcome

# Create and validate a document
doc = Doc(
    doc_id="doc_AAAAAAAAAAAAAAAAAAAAAA",
    source_uri="https://example.com/case1",
    raw_text="ACME stated: \"We did nothing wrong.\"",
    meta={"court": "N.D.Cal", "docket": "1:24-cv-00001", "party": "ACME"}
)

# Validation happens automatically
print(doc.schema_version)  # "1.0"
```

### CLI Validation

```bash
# Validate JSONL files
corpus-validate jsonl Doc data/docs.jsonl
corpus-validate jsonl Quote data/quotes.jsonl

# Generate JSON schemas
corpus-validate generate-schemas schemas/

# List available models
corpus-validate list-models
```

## Data Models

### Core Models

- **`Doc`**: Document with raw text and metadata
- **`Quote`**: Extracted quote with span and speaker info
- **`Outcome`**: Case outcome labels and metadata

### Feature Models

- **`QuoteFeatures`**: Feature vectors for quotes (versioned)
- **`CaseVector`**: Aggregated case-level features

### Prediction Models

- **`Prediction`**: ML predictions (quote or case level)
- **`CasePrediction`**: Case-level predictions with metadata

## ID Generation

IDs are generated using BLAKE3 hashing for optimal performance and collision resistance:

- **Prefix-based**: `doc_`, `q_`, `case_` prefixes for easy identification
- **Deterministic**: Same input always produces same ID
- **URL-safe**: Base64-encoded for web compatibility
- **Namespace-aware**: Includes court/source information to avoid collisions

## Versioning

- **Schema versioning**: `schema_version` field in all models
- **Package versioning**: SemVer (MAJOR.MINOR.PATCH)
- **Breaking changes**: Bump MAJOR version and schema_version
- **Additive changes**: Bump MINOR version
- **Fixes**: Bump PATCH version

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run type checking
mypy src

# Generate schemas
python -c "from corpus_types.utils.export_schema import export_all; export_all()"
```

## License

MIT
