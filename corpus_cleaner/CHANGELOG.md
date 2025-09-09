# Changelog

All notable changes to corpus-cleaner will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-01-15

### Added
- Initial release of corpus-cleaner module
- TextCleaner class with comprehensive normalization capabilities
- Offset mapping preservation for span alignment
- CLI interface for document normalization
- Legal text-aware processing (citations, case names, court terminology)
- Unicode normalization and character handling
- Configurable processing rules via YAML
- Comprehensive test suite with fixtures
- Documentation and examples

### Features
- Deterministic text normalization
- Character and word-level offset tracking
- Batch processing with memory optimization
- Legal document formatting preservation
- Unicode and encoding handling
- Configurable filtering and cleaning rules
- JSONL streaming for large datasets

## [0.1.0] - 2024-01-01

### Added
- Basic TextCleaner implementation
- Offset mapping functionality
- Initial CLI interface
- Basic test framework
- Configuration file structure
