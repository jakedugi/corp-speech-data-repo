#!/usr/bin/env python3
"""
CLI interface for corpus-hydrator module.

Usage:
    python -m corpus_hydrator.cli.fetch courtlistener --query query.yaml --output-dir output/
    python -m corpus_hydrator.cli.fetch rss --feeds feeds.yaml --output-dir output/
    python -m corpus_hydrator.cli.fetch wikipedia --pages pages.yaml --output-dir output/
"""

from pathlib import Path
from typing import Optional
import typer
import logging
import yaml

from ..pipeline import run_courtlistener_fetch, run_rss_fetch, run_wikipedia_scrape

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def courtlistener(
    query_file: Path = typer.Option(
        ..., "--query", help="Query configuration YAML file"
    ),
    output_dir: Path = typer.Option(
        ..., "--output-dir", help="Output directory for documents"
    ),
    fixture_file: Optional[Path] = typer.Option(
        None, "--use-fixture", help="Use fixture file instead of API calls"
    ),
    api_key: Optional[str] = typer.Option(
        None,
        "--api-key",
        help="CourtListener API key (or set COURT_LISTENER_API_KEY env var)",
    ),
):
    """
    Fetch documents from CourtListener API based on query configuration.
    Use --use-fixture for offline testing.
    """
    logger.info(f"Fetching documents from CourtListener using query: {query_file}")
    logger.info(f"Output directory: {output_dir}")

    # Load query configuration
    with open(query_file) as f:
        query_config = yaml.safe_load(f)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run pipeline
    run_courtlistener_fetch(
        query_config=query_config,
        output_dir=output_dir,
        api_key=api_key,
        fixture_file=fixture_file,
    )


@app.command()
def rss(
    feeds_file: Path = typer.Option(
        ..., "--feeds", help="RSS feeds configuration YAML file"
    ),
    output_dir: Path = typer.Option(
        ..., "--output-dir", help="Output directory for documents"
    ),
    max_entries: int = typer.Option(
        100, "--max-entries", help="Maximum entries per feed"
    ),
):
    """
    Fetch documents from RSS feeds.
    """
    logger.info(f"Fetching documents from RSS feeds: {feeds_file}")
    logger.info(f"Output directory: {output_dir}")

    # Load feeds configuration
    with open(feeds_file) as f:
        feeds_config = yaml.safe_load(f)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run pipeline
    run_rss_fetch(
        feeds_config=feeds_config,
        output_dir=output_dir,
        max_entries=max_entries,
    )


@app.command()
def wikipedia(
    pages_file: Path = typer.Option(
        ..., "--pages", help="Wikipedia pages configuration YAML file"
    ),
    output_dir: Path = typer.Option(
        ..., "--output-dir", help="Output directory for documents"
    ),
):
    """
    Scrape documents from Wikipedia pages.
    """
    logger.info(f"Scraping documents from Wikipedia pages: {pages_file}")
    logger.info(f"Output directory: {output_dir}")

    # Load pages configuration
    with open(pages_file) as f:
        pages_config = yaml.safe_load(f)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run pipeline
    run_wikipedia_scrape(
        pages_config=pages_config,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    app()
