# Changelog

All notable changes to corpus-types will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-01-XX

### Added
- Initial release of corpus-types package
- Pydantic schemas for Doc, Quote, and Outcome data types
- Deterministic ID generation utilities (BLAKE3)
- JSON Schema validation and export
- CLI validation tools

### Features
- **Doc Schema**: Raw and normalized document validation
- **Quote Schema**: Extracted quotes with speaker attribution
- **Outcome Schema**: Case outcomes and labels
- **ID Generation**: Collision-resistant deterministic IDs
- **Schema Export**: JSON Schema generation for external tools

### Technical
- Python 3.10+ support
- Pydantic 2.x for data validation
- BLAKE3 for cryptographic hashing
- Comprehensive type hints and documentation

## [0.1.0] - 2024-01-XX

### Added
- Basic schema definitions
- Initial validation utilities
- Development setup and testing framework