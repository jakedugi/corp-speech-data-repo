# Data Contracts

This document describes the data schemas and contracts used throughout the pipeline. All artifacts are validated against these Pydantic schemas.

## Overview

The pipeline uses three primary data types:
- **Doc**: Documents and their metadata
- **Quote**: Extracted quotes with speaker attribution
- **Outcome**: Case outcomes and labels

## Doc Schema

Documents from any source (CourtListener, RSS, web scraping).

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | string | ✅ | Schema version (currently "1.0") |
| `doc_id` | string | ✅ | Unique document identifier |
| `source_uri` | string | ✅ | Original source URL |
| `retrieved_at` | datetime | ✅ | When document was retrieved |
| `raw_text` | string | ✅ | Full document text content |
| `meta` | object | ✅ | Document metadata |
| `provenance` | object | ✅ | Complete provenance information |

### Sample Doc

```json
{
  "schema_version": "1.0",
  "doc_id": "doc_abc123def456",
  "source_uri": "https://www.courtlistener.com/opinion/12345/test/",
  "retrieved_at": "2024-01-01T12:00:00Z",
  "raw_text": "The Supreme Court held that the corporation's securities filing was inadequate under Section 10(b) of the Securities Exchange Act. Justice Roberts wrote that 'the disclosure requirements are clear and must be followed.' The Court dismissed the appeal.",
  "meta": {
    "court": "scotus",
    "docket": "22-123",
    "party": "SEC v. Corporation"
  },
  "provenance": {
    "source": "courtlistener",
    "source_uri": "https://www.courtlistener.com/opinion/12345/test/",
    "retrieved_at": "2024-01-01T12:00:00Z",
    "request": {"endpoint": "opinions"},
    "response": {"http_status": 200, "sha256": "abc123", "bytes": 1024},
    "adapter": {"name": "corpus-hydrator", "version": "1.0.0"},
    "provider": {"opinion_id": 12345, "cluster_id": 12345}
  }
}
```

## Quote Schema

Extracted quotes with speaker attribution and confidence scores.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | string | ✅ | Schema version (currently "1.0") |
| `quote_id` | string | ✅ | Unique quote identifier |
| `doc_id` | string | ✅ | Parent document ID |
| `text` | string | ✅ | Normalized quote text |
| `context` | string | ❌ | Surrounding context |
| `speaker` | string | ❌ | Detected speaker |
| `score` | float | ❌ | Extraction confidence (0-1) |
| `span` | object | ❌ | Character positions in document |
| `urls` | array | ❌ | Source URLs |
| `stage` | integer | ❌ | Processing stage |

### Sample Quote

```json
{
  "schema_version": "1.0",
  "quote_id": "q_def456ghi789",
  "doc_id": "doc_abc123def456",
  "text": "the disclosure requirements are clear and must be followed",
  "context": "Justice Roberts wrote that 'the disclosure requirements are clear and must be followed.' The Court dismissed the appeal.",
  "speaker": "Justice Roberts",
  "score": 0.95,
  "span": {
    "start": 25,
    "end": 46
  },
  "urls": ["https://www.courtlistener.com/opinion/12345/test/"],
  "stage": 1
}
```

## Outcome Schema

Case outcomes with classification and confidence.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | string | ✅ | Schema version (currently "1.0") |
| `case_id` | string | ✅ | Unique case identifier |
| `label` | string | ✅ | Outcome label (win/loss/settlement/dismissal/mixed/unknown) |
| `label_source` | string | ✅ | How label was determined |
| `confidence` | float | ❌ | Classification confidence (0-1) |
| `date` | date | ❌ | Outcome date |
| `meta` | object | ❌ | Additional metadata |

### Sample Outcome

```json
{
  "schema_version": "1.0",
  "case_id": "case_xyz789abc123",
  "label": "win",
  "label_source": "manual",
  "confidence": 0.92,
  "date": "2023-06-15",
  "meta": {
    "court": "scotus",
    "docket": "22-123",
    "judge": "Justice Roberts"
  }
}
```

## ID Generation Rules

### Document IDs (`doc_id`)
- Format: `doc_{blake3_hash}`
- Based on: `source_uri + retrieved_at`
- Deterministic: Same source + time = same ID
- Length: 16-character hash (64-bit collision resistance)

### Quote IDs (`quote_id`)
- Format: `q_{blake3_hash}`
- Based on: `doc_id + start + end + normalized_text`
- Deterministic: Same span + text = same ID
- Preserves quote uniqueness within documents

### Case IDs (`case_id`)
- Format: `case_{blake3_hash}`
- Based on: `doc_id + case_metadata`
- Deterministic: Same case info = same ID
- Enables cross-document case linking

## Validation Rules

### Required Validations
- All IDs must be unique within their artifact type
- Foreign keys must reference existing records
- Schema versions must match expected values
- Timestamps must be valid ISO format
- URLs must be well-formed
- Text fields cannot be empty or whitespace-only

### Data Integrity Guarantees
- **No duplicates**: Each artifact type has unique primary keys
- **Referential integrity**: All foreign keys are valid
- **Deterministic**: Same inputs produce identical outputs
- **Versioned**: Schema changes are explicitly versioned
- **Typed**: All fields validated against strict types

## Schema Evolution

### Versioning Strategy
- **Patch** (1.0.x): Bug fixes, no schema changes
- **Minor** (1.x.0): Additive changes, backward compatible
- **Major** (x.0.0): Breaking changes, migration required

### Migration Support
- Old schema versions remain readable
- Migration scripts provided for major version changes
- Validation supports multiple schema versions
- Clear deprecation warnings for old formats

## Export Formats

### JSON Schema
All Pydantic models can be exported as JSON Schema:

```bash
# Export schemas
python -c "from corpus_types.schemas.models import Doc; print(Doc.model_json_schema())"
```

### Sample Data
Each package includes sample fixtures:
- `fixtures/docs.raw.small.jsonl` - Raw documents
- `fixtures/quotes.small.jsonl` - Extracted quotes
- `fixtures/outcomes.small.jsonl` - Case outcomes

## Quality Metrics

### Validation Coverage
- **Schema validation**: 100% of fields validated
- **Type safety**: Strict typing enforced
- **Business rules**: Domain-specific constraints
- **Data quality**: Completeness and consistency checks

### Performance
- **Validation speed**: < 1ms per record
- **Memory usage**: Minimal overhead
- **Streaming support**: Large datasets supported
- **Parallel validation**: Multi-worker support

## Troubleshooting

### Common Validation Errors
- **Missing required fields**: Check data source completeness
- **Invalid IDs**: Verify ID generation logic
- **Foreign key violations**: Ensure proper data loading order
- **Type mismatches**: Validate data source formats

### Schema Validation Commands
```bash
# Validate specific artifact
corpus-validate jsonl Doc data/docs.norm.jsonl
corpus-validate jsonl Quote data/quotes.jsonl
corpus-validate jsonl Outcome data/outcomes.jsonl

# Get detailed error report
corpus-validate jsonl Doc data/docs.norm.jsonl --verbose
```
