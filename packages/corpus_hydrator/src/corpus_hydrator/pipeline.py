"""
High-level pipeline for corpus hydration.

This module provides the main run() functions used by the CLI.
"""

import logging
from pathlib import Path
from typing import List, Optional

# Comment out problematic imports for now to focus on Wikipedia scraper
# from .adapters.courtlistener.courtlistener_client import CourtListenerClient
# from .adapters.rss.rss_client import RSSClient
from .adapters.wikipedia.scraper import WikipediaScraper

# from .config.courtlistener_config import CourtListenerConfig
# from .config.rss_config import RSS_FEEDS

logger = logging.getLogger(__name__)


def run_courtlistener_fetch(
    query_config: dict,
    output_dir: Path,
    api_key: Optional[str] = None,
    fixture_file: Optional[Path] = None,
) -> None:
    """
    Fetch documents from CourtListener API.

    Args:
        query_config: Query configuration dictionary
        output_dir: Output directory for fetched documents
        api_key: Optional API key
        fixture_file: Optional fixture file for testing
    """
    if fixture_file:
        logger.info(f"Using fixture file: {fixture_file}")
        # Copy fixture to output (simulating fetch)
        import shutil

        shutil.copy2(fixture_file, output_dir / "documents.jsonl")
        return

    logger.info("Fetching documents from CourtListener")
    client = CourtListenerClient(api_key=api_key)

    # Execute query based on config
    documents = client.search_opinions(query_config.get("courtlistener", {}))

    # Write output
    output_file = output_dir / "documents.jsonl"
    with open(output_file, "w") as f:
        import json

        for doc in documents:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    logger.info(f"Successfully fetched {len(documents)} documents")


def run_rss_fetch(
    feeds_config: dict,
    output_dir: Path,
    max_entries: int = 100,
) -> None:
    """
    Fetch documents from RSS feeds.

    Args:
        feeds_config: RSS feeds configuration dictionary
        output_dir: Output directory for fetched documents
        max_entries: Maximum entries per feed
    """
    logger.info("Fetching documents from RSS feeds")
    client = RSSClient()

    # Fetch from all feeds
    all_documents = []
    for feed_name, feed_config in feeds_config.get("feeds", {}).items():
        documents = client.fetch_feed(feed_config["url"], max_entries=max_entries)
        all_documents.extend(documents)

    # Write output
    output_file = output_dir / "documents.jsonl"
    with open(output_file, "w") as f:
        import json

        for doc in all_documents:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    logger.info(f"Successfully fetched {len(all_documents)} documents from RSS feeds")


def run_wikipedia_scrape(
    pages_config: dict,
    output_dir: Path,
) -> None:
    """
    Scrape company executive data from Wikipedia market indices.

    Args:
        pages_config: Configuration with index name (e.g., {"index": "sp500"})
        output_dir: Output directory for scraped data
    """
    logger.info("Scraping company executive data from Wikipedia")

    # Import here to avoid circular imports
    from corpus_types.schemas.scraper import get_default_config

    # Extract index name from config, default to sp500
    index_name = pages_config.get("index", "sp500")

    # Create configuration
    config = get_default_config()
    config.enabled_indices = [index_name]
    config.dry_run = pages_config.get("dry_run", False)
    config.scraping.max_companies = pages_config.get("max_companies", None)
    config.scraping.output_dir = str(output_dir)

    # Create and run scraper
    scraper = WikipediaScraper(config)

    # Scrape companies
    companies, result = scraper.scrape_index(index_name)

    if not companies:
        logger.warning(f"No companies found for index {index_name}")
        return

    logger.info(f"Found {len(companies)} companies")

    # Scrape executives
    logger.info("Scraping executive information...")
    officers = scraper.scrape_executives_for_companies(companies)

    total_officers = sum(len(officers_list) for officers_list in officers.values())
    logger.info(f"Found {total_officers} executives across {len(officers)} companies")

    # Save results (this will create the CSV files)
    scraper.save_results(companies, officers, index_name)

    logger.info(f"Successfully scraped executive data from {index_name} index")
