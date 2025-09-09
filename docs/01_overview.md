# Overview

## What This Repository Does

The Corporate Speech Data Repository is a production-ready data pipeline that transforms raw legal documents into structured datasets for corporate speech legal risk analysis. It processes court opinions, RSS feeds, and web content to extract quotes, speakers, and case outcomes.

## Architecture

The pipeline follows a clean separation of concerns with four specialized packages:

### 1. corpus-types
**Purpose**: Data contracts and validation
- Pydantic schemas for all data types (Doc, Quote, Outcome)
- Deterministic ID generation utilities
- JSON Schema validation and export
- CLI tools for data validation

### 2. corpus-hydrator
**Purpose**: Data collection and ingestion
- CourtListener API client
- RSS feed parsing
- Web scraping capabilities
- Rate limiting and retry logic
- Offline fixture support for testing

### 3. corpus-cleaner
**Purpose**: Text normalization and preprocessing
- Unicode normalization (NFC)
- Whitespace and formatting cleanup
- Character offset mapping preservation
- Deterministic text transformations

### 4. corpus-extractors
**Purpose**: Information extraction using ML
- Quote detection and speaker attribution
- Case outcome classification
- Confidence scoring and quality assessment
- Semantic reranking and deduplication

## Key Features

### 🔒 Deterministic & Reproducible
- Same inputs always produce same outputs
- BLAKE3 fingerprints for data integrity verification
- Stable, collision-resistant IDs

### ✅ Validated & Typed
- Strict Pydantic schemas for all data
- Runtime validation with detailed error messages
- Type-safe data contracts between components

### 🚀 Production-Ready
- Comprehensive error handling and logging
- Configurable timeouts and retry logic
- Environment-based configuration
- Offline testing capabilities

### 🧪 Well-Tested
- Unit tests for all core functionality
- Integration tests for CLI interfaces
- End-to-end pipeline testing
- Schema validation testing

## Data Flow

```
Query Config → Hydrator → Raw Docs → Cleaner → Normalized Docs → Extractors → Quotes & Outcomes → Validator
     ↓           ↓            ↓           ↓            ↓               ↓             ↓               ↓
  YAML        API/Files    JSONL       JSONL         JSONL          JSONL         JSONL         Reports
```

## Quality Guarantees

### Data Integrity
- All artifacts validated against schemas
- Foreign key relationships maintained
- No duplicate records within artifacts
- Consistent ID generation across runs

### Performance
- Streaming processing for large datasets
- Memory-efficient text processing
- Configurable batch sizes and timeouts
- Parallel processing where appropriate

### Reliability
- Comprehensive error handling
- Graceful degradation on failures
- Detailed logging for debugging
- Idempotent operations (safe to re-run)

## Use Cases

### Research & Analysis
- Corporate speech pattern analysis
- Legal risk assessment
- Speaker attribution studies
- Case outcome prediction

### Production Applications
- Automated document processing
- Real-time legal monitoring
- Risk assessment dashboards
- Compliance reporting

### Development & Testing
- ML model training data
- Validation dataset creation
- Integration testing fixtures
- Performance benchmarking
