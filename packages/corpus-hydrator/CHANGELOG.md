# Changelog

All notable changes to corpus-hydrator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-01-XX

### Added
- Initial release of corpus-hydrator package
- CourtListener API client with rate limiting
- RSS feed parsing and article extraction
- Web scraping capabilities with Playwright
- Offline fixture support for testing
- CLI interface for data collection

### Features
- **API Integration**: CourtListener REST API v4 support
- **Feed Processing**: SEC, FTC, DOJ RSS feed parsing
- **Web Scraping**: Wikipedia and legal document scraping
- **Rate Limiting**: Built-in request throttling and retry logic
- **Provenance Tracking**: Request/response metadata capture
- **Fixture Support**: Offline testing with pre-recorded data

### Technical
- Python 3.10+ support
- HTTPX for async HTTP requests
- Playwright for browser automation
- Feedparser for RSS processing
- BeautifulSoup4 for HTML parsing

## [0.1.0] - 2024-01-XX

### Added
- Basic API client structure
- Initial RSS feed support
- Development setup and testing framework