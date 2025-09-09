# Changelog

All notable changes to corpus-api will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-01-15

### Added
- Initial release of corpus-api module
- CourtListener API client with full search and retrieval capabilities
- RSS feed client for SEC, FTC, and DOJ news sources
- Wikipedia scraper for legal case lists and corporate law content
- Unified CLI interface for all data sources
- Comprehensive configuration system with YAML files
- Rate limiting and retry logic for all APIs
- Pydantic models for data validation
- Extensive test suite with fixtures
- Documentation and examples

### Features
- CourtListener opinion search and retrieval
- RSS feed parsing and article extraction
- Wikipedia page scraping with section targeting
- JSONL output format for all sources
- Metadata extraction and standardization
- Error handling and recovery
- Logging with Loguru

## [0.1.0] - 2024-01-01

### Added
- Basic CourtListener API client
- RSS feed parsing functionality
- Initial CLI interface
- Basic test framework
- Configuration file structure
