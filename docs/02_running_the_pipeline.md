# Running the Pipeline

This guide shows you how to run the complete data pipeline from raw data to validated outputs.

## Prerequisites

```bash
# Install all packages
uv pip install -e packages/corpus-types packages/corpus-hydrator packages/corpus-cleaner packages/corpus-extractors

# Or with pip
pip install -e packages/corpus-types packages/corpus-hydrator packages/corpus-cleaner packages/corpus-extractors
```

## Quick Start (One Command)

```bash
make demo_e2e
```

This runs the complete pipeline and produces all artifacts.

## Step-by-Step Execution

### 1. Clean Workspace

```bash
make clean
```

Creates a fresh `data/` directory for outputs.

### 2. Fetch Raw Documents

```bash
make fetch
# Or manually:
hydrator fetch --query configs/query.small.yaml --out data/docs.raw.jsonl
```

**Input**: Query configuration (`configs/query.small.yaml`)
**Output**: Raw documents (`data/docs.raw.jsonl`)

Sample output:
```json
{
  "schema_version": "1.0",
  "doc_id": "doc_abc123...",
  "source_uri": "https://www.courtlistener.com/opinion/12345/",
  "raw_text": "The Supreme Court held that...",
  "meta": {"court": "scotus", "docket": "22-123"},
  "provenance": {...}
}
```

### 3. Normalize Documents

```bash
make normalize
# Or manually:
cleaner normalize --in data/docs.raw.jsonl --out data/docs.norm.jsonl --keep-offset-map
```

**Input**: Raw documents
**Output**: Normalized documents with preserved character offsets

Changes applied:
- Unicode normalization (NFC)
- Whitespace cleanup
- Hyphenation handling
- Footnote marker removal

### 4. Extract Quotes

```bash
extract quotes --in data/docs.norm.jsonl --out data/quotes.jsonl
```

**Input**: Normalized documents
**Output**: Extracted quotes with speaker attribution

Sample output:
```json
{
  "schema_version": "1.0",
  "quote_id": "q_def456...",
  "doc_id": "doc_abc123...",
  "text": "the precedent is clear",
  "speaker": "Justice Roberts",
  "score": 0.95,
  "span": {"start": 25, "end": 46},
  "context": "Justice Roberts wrote that..."
}
```

### 5. Extract Outcomes

```bash
extract outcomes --in data/docs.norm.jsonl --out data/outcomes.jsonl
```

**Input**: Normalized documents
**Output**: Case outcomes and labels

Sample output:
```json
{
  "schema_version": "1.0",
  "case_id": "case_ghi789...",
  "label": "win",
  "label_source": "manual",
  "confidence": 0.92,
  "date": "2023-06-15"
}
```

### 6. Validate Outputs

```bash
make validate
# Or manually:
corpus-validate jsonl Doc data/docs.norm.jsonl
corpus-validate jsonl Quote data/quotes.jsonl
corpus-validate jsonl Outcome data/outcomes.jsonl
```

Validates all outputs against their schemas.

### 7. Generate Manifest

```bash
make manifest
```

Creates `data/manifest.json` with:
- Tool versions used
- Record counts per artifact
- BLAKE3 fingerprints for reproducibility

## Common Flags and Options

### Hydrator Options
```bash
# Use fixtures instead of API calls (for offline/testing)
hydrator fetch --use-fixture fixtures/docs.raw.small.jsonl --out data/docs.raw.jsonl

# Specify API key
HYDRATOR_API_KEY=your_key hydrator fetch --query configs/query.yaml --out data/docs.raw.jsonl

# Configure timeouts
REQUESTS_TIMEOUT=60 hydrator fetch --query configs/query.yaml --out data/docs.raw.jsonl
```

### Cleaner Options
```bash
# Skip offset mapping (faster but less accurate)
cleaner normalize --in data/docs.raw.jsonl --out data/docs.norm.jsonl

# Custom configuration
cleaner normalize --config configs/cleaner.custom.yaml --in data/docs.raw.jsonl --out data/docs.norm.jsonl
```

### Extractor Options
```bash
# Limit processing for testing
extract quotes --in data/docs.norm.jsonl --out data/quotes.jsonl --limit 100

# Custom confidence threshold
extract quotes --in data/docs.norm.jsonl --out data/quotes.jsonl --min-confidence 0.8
```

## Configuration Files

### Query Configuration (`configs/query.small.yaml`)
```yaml
sources:
  - courtlistener

courtlistener:
  date_range:
    start: "2023-01-01"
    end: "2023-06-30"
  courts: ["scotus"]
  keywords: ["corporation", "securities"]
  max_results: 50
```

### Environment Variables
```bash
# API Configuration
HYDRATOR_API_KEY=your_courtlistener_api_key
REQUESTS_TIMEOUT=30
MAX_RETRIES=3

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Processing
BATCH_SIZE=100
MAX_WORKERS=4
```

## Offline Development

For development without network access:

```bash
# Use fixture data
make clean
hydrator fetch --use-fixture fixtures/docs.raw.small.jsonl --out data/docs.raw.jsonl
make normalize extract validate manifest
```

## Expected Outputs

After successful pipeline run:

```
data/
├── docs.raw.jsonl      # Raw documents (JSONL)
├── docs.norm.jsonl     # Normalized documents (JSONL)
├── quotes.jsonl        # Extracted quotes (JSONL)
├── outcomes.jsonl      # Case outcomes (JSONL)
├── manifest.json       # Metadata and fingerprints (JSON)
└── RUN.md             # Execution log (Markdown)
```

## Performance Notes

- **Small demo**: ~30 seconds on modern hardware
- **Full dataset**: Scales linearly with document count
- **Memory usage**: ~100MB for 10k documents
- **Network**: ~1MB/s download speed (CourtListener API limits)

## Troubleshooting

See [docs/04_troubleshooting.md](04_troubleshooting.md) for common issues and solutions.
