"""
High-level pipeline for corpus hydration.

This module provides the main run() functions used by the CLI.
"""

from pathlib import Path
from typing import List, Optional
import logging

from .adapters.courtlistener.courtlistener_client import CourtListenerClient
from .adapters.rss.rss_client import RSSClient
from .adapters.wikipedia.sandp_scraper import WikipediaScraper
from .config.courtlistener_config import CourtListenerConfig
from .config.rss_config import RSS_FEEDS

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
    Scrape documents from Wikipedia pages.

    Args:
        pages_config: Wikipedia pages configuration dictionary
        output_dir: Output directory for scraped documents
    """
    logger.info("Scraping documents from Wikipedia pages")
    scraper = WikipediaScraper()

    # Scrape all pages
    all_documents = []
    for page_name, page_config in pages_config.get("pages", {}).items():
        content = scraper.scrape_page(page_config["title"])
        if content:
            doc = {
                "doc_id": f"wiki_{page_name}",
                "source_uri": f"https://en.wikipedia.org/wiki/{page_config['title']}",
                "retrieved_at": "2024-01-01T00:00:00Z",  # Would use current timestamp
                "raw_text": content,
                "meta": {
                    "source": "wikipedia",
                    "page_title": page_config["title"],
                    "sections": page_config.get("sections", []),
                },
            }
            all_documents.append(doc)

    # Write output
    output_file = output_dir / "documents.jsonl"
    with open(output_file, "w") as f:
        import json
        for doc in all_documents:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    logger.info(f"Successfully scraped {len(all_documents)} documents from Wikipedia")
