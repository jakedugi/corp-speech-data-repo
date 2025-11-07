#!/usr/bin/env python3
"""
CLI interface for corpus_hydrator module.

Usage:
    # CourtListener API fetching
    python -m corpus_hydrator.cli.fetch courtlistener --query query.yaml --output-dir output/

    # RSS feed fetching
    python -m corpus_hydrator.cli.fetch rss --feeds feeds.yaml --output-dir output/

    # Wikipedia scraping (executives)
    python -m corpus_hydrator.cli.fetch wikipedia --index sp500 --output-dir data/

    # Index constituents from Wikipedia (RECOMMENDED)
    python -m corpus_hydrator.cli.fetch index-constituents --index sp500 --output-dir data/

    # Individual Wikipedia pages
    python -m corpus_hydrator.cli.fetch wikipedia_pages --pages pages.yaml --output-dir output/
"""

import logging
from pathlib import Path
from typing import Optional

import typer
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
    index: str = typer.Option(
        "sp500",
        "--index",
        help="Market index to scrape (sp500, dow, nasdaq100, russell1000)",
    ),
    output_dir: Path = typer.Option(
        ..., "--output-dir", help="Output directory for scraped data"
    ),
    max_companies: Optional[int] = typer.Option(
        None, "--max-companies", help="Maximum companies to scrape"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Run without making HTTP requests"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
):
    """
    Scrape company executive data from Wikipedia market index pages.

    This command scrapes company lists and executive information from Wikipedia
    pages for various market indices (S&P 500, Dow Jones, NASDAQ-100, etc.).
    """
    logger.info(f"Scraping {index} companies and executives from Wikipedia")
    logger.info(f"Output directory: {output_dir}")

    # Import here to avoid circular imports
    from corpus_types.schemas.scraper import get_default_config

    from ..adapters.wikipedia.scraper import WikipediaScraper

    # Get configuration
    config = get_default_config()
    config.enabled_indices = [index]
    config.dry_run = dry_run
    config.verbose = verbose

    if max_companies:
        if index in config.indices:
            config.indices[index].max_companies = max_companies

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create and run scraper
    scraper = WikipediaScraper(config)

    # Scrape companies
    companies, result = scraper.scrape_index(index)

    if not companies:
        logger.error(f"No companies found for index {index}")
        return

    logger.info(f"Found {len(companies)} companies")

    # Scrape executives
    logger.info("Scraping executive information...")
    officers = scraper.scrape_executives_for_companies(companies)

    total_officers = sum(len(officer_list) for officer_list in officers.values())
    logger.info(f"Found {total_officers} executives across {len(officers)} companies")

    # Save results
    scraper.save_results(companies, officers, index)

    logger.info("Wikipedia scraping completed successfully!")


@app.command()
def wikipedia_pages(
    pages_file: Path = typer.Option(
        ..., "--pages", help="Wikipedia pages configuration YAML file"
    ),
    output_dir: Path = typer.Option(
        ..., "--output-dir", help="Output directory for documents"
    ),
):
    """
    Scrape documents from individual Wikipedia pages.

    This is the legacy command for scraping specific Wikipedia pages for content.
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


@app.command()
def index_constituents(
    index: str = typer.Option(
        "sp500", "--index", help="Index to extract (sp500, dow, nasdaq100, all)"
    ),
    output_dir: Path = typer.Option(
        "data", "--output-dir", help="Output directory for files and cache"
    ),
    formats: list[str] = typer.Option(
        ["csv", "parquet"], "--format", "-f", help="Output formats (csv, parquet)"
    ),
    cache_ttl: int = typer.Option(
        21600, "--cache-ttl", help="Cache TTL in seconds (default: 6 hours)"
    ),
    force: bool = typer.Option(False, "--force", help="Force refresh (ignore cache)"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
):
    """
    Extract authoritative index constituents with clean architecture.

    Features:
    - Multiple data sources (Wikipedia primary, FMP/Yahoo ETF fallbacks)
    - Intelligent caching with ETag support
    - Deterministic output with SHA256 manifests
    - Multiple output formats (CSV, Parquet)
    - Graceful error handling and fallbacks

    Supported indexes:
    - S&P 500: Symbol, Security, GICS Sector, Date first added
    - Dow Jones: Symbol, Company, Industry, Date added
    - Nasdaq 100: Symbol, Company, Industry, Date added

    Examples:
        python -m corpus_hydrator.cli.fetch index-constituents --index sp500
        python -m corpus_hydrator.cli.fetch index-constituents --index all --format csv parquet
        python -m corpus_hydrator.cli.fetch index-constituents --index sp500 --force --verbose
    """
    import pandas as pd

    try:
        # Import new clean architecture components
        from ..adapters.index_constituents import (
            INDEX_CONFIGS,
            HtmlTableParser,
            WikipediaProvider,
            extract_index,
            extract_multiple_indexes,
            write_bundle,
        )

        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)

        logger.info(f"Extracting index constituents for: {index}")

        # Setup caching directory if TTL is specified
        cache_dir = output_dir / ".cache" if cache_ttl > 0 else None

        # Initialize components following clean architecture
        provider = WikipediaProvider(cache_dir=cache_dir, force_refresh=force)
        parser = HtmlTableParser()

        output_dir.mkdir(parents=True, exist_ok=True)

        if index == "all":
            logger.info("Extracting all index constituents...")
            index_keys = list(INDEX_CONFIGS.keys())
            results = extract_multiple_indexes(index_keys, provider, parser)

            success_count = 0
            for index_key, result in results.items():
                if result.success and result.constituents:
                    # Write bundle with manifest
                    manifest = write_bundle(result.constituents, output_dir, formats)
                    success_count += 1
                    logger.info(
                        f"Saved {result.total_constituents} companies for {result.index_name}"
                    )

                    # Show sample for each index
                    df = pd.DataFrame([c.dict() for c in result.constituents])
                    print(
                        f"\n=== {result.index_name.upper()} ({result.total_constituents} companies) ==="
                    )
                    print(df.head().to_string(index=False))

                    # Show manifest info
                    if manifest:
                        print(
                            f"Manifest: {result.index_name.lower().replace(' ', '')}_manifest.json"
                        )
                        if "sha256_csv" in manifest:
                            print(f"   CSV SHA256: {manifest['sha256_csv'][:16]}...")
                else:
                    error_msg = result.error_message or "Unknown error"
                    logger.error(f"Failed to extract {index_key}: {error_msg}")

            if success_count == 0:
                logger.error("No indexes were successfully extracted")
                raise typer.Exit(1)

            print(f"\nSuccessfully extracted {success_count}/{len(index_keys)} indexes")

        else:
            result = extract_index(index, provider, parser)

            if result.success and result.constituents:
                # Write bundle with manifest
                manifest = write_bundle(result.constituents, output_dir, formats)
                logger.info(
                    f"Saved {result.total_constituents} companies for {result.index_name}"
                )

                # Show results
                df = pd.DataFrame([c.dict() for c in result.constituents])

                # Show the exact format the user requested for S&P 500
                if index == "sp500":
                    print("\n=== S&P 500 Constituents (as requested) ===")
                    print(
                        'sp500 = sp500[["Symbol", "Security", "GICS Sector", "Date first added"]]'
                    )
                    print("print(sp500.head())")

                    # Filter to legacy format for display
                    display_df = df[
                        ["symbol", "company_name", "sector", "date_added"]
                    ].copy()
                    display_df.columns = [
                        "Symbol",
                        "Security",
                        "GICS Sector",
                        "Date first added",
                    ]
                    print(display_df.head().to_string(index=False))
                else:
                    print(
                        f"\n=== {result.index_name.upper()} Constituents ({result.total_constituents} companies) ==="
                    )
                    print(df.head().to_string(index=False))

                # Show manifest info
                if manifest:
                    print(
                        f"\nManifest: {result.index_name.lower().replace(' ', '')}_manifest.json"
                    )
                    print(f"   SHA256: {manifest.get('sha256_csv', 'N/A')[:16]}...")
                    print(f"   Source: {manifest.get('source_url', 'N/A')}")
                    print(f"   Extracted: {manifest.get('extracted_at', 'N/A')}")

            else:
                error_msg = result.error_message or "Unknown error"
                logger.error(f"Failed to extract {index} constituents: {error_msg}")
                raise typer.Exit(1)

    except ImportError as e:
        logger.error(f"Import error: {e}")
        logger.error("Make sure the index_constituents adapter is properly installed")
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
