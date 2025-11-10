#!/usr/bin/env python3
"""
CLI interface for corpus_hydrator module.

Usage:
    # CourtListener API fetching
    python -m corpus_hydrator.cli.fetch courtlistener --query query.yaml --output-dir output/

    # RSS feed fetching
    python -m corpus_hydrator.cli.fetch rss --feeds feeds.yaml --output-dir output/

    # Wikipedia key people extraction (unified orchestrator)
    python -m corpus_hydrator.cli.fetch orchestrator --index dow,sp500,nasdaq100 --output-dir data/
"""

import logging
from pathlib import Path
from typing import List, Optional

import typer
import yaml

from ..pipeline import run_courtlistener_fetch, run_rss_fetch

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


# REMOVED: Broken wikipedia and wikipedia_pages commands


# REMOVED: index-constituents command - functionality consolidated into orchestrator
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
def orchestrator(
    # Mandatory parameters
    index: List[str] = typer.Option(
        ["dow"], "--index", "-i",
        help="Index(es) to scrape (dow, sp500, nasdaq100). Can specify multiple with --index dow --index sp500 or comma-separated --index dow,sp500"
    ),
    output_dir: str = typer.Option(
        ..., "--output-dir", help="Output directory for scraped data"
    ),

    # Extraction control
    extract_companies: bool = typer.Option(
        True, "--companies/--no-companies",
        help="Extract company information with business metadata"
    ),
    extract_people: bool = typer.Option(
        True, "--people/--no-people",
        help="Extract key people information"
    ),
    extract_roles: bool = typer.Option(
        True, "--roles/--no-roles",
        help="Extract role definitions"
    ),
    extract_appointments: bool = typer.Option(
        True, "--appointments/--no-appointments",
        help="Extract person-company-role relationships"
    ),

    # Optional parameters
    max_companies: Optional[int] = typer.Option(
        None, "--max-companies",
        help="Limit number of companies to process (default: all)"
    ),
    workers: int = typer.Option(
        4, "--workers",
        help="Number of parallel workers"
    ),
    verbose: bool = typer.Option(
        True, "--verbose/--no-verbose", "-v",
        help="Enable verbose logging"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Preview commands without executing"
    ),
    resume: bool = typer.Option(
        False, "--resume",
        help="Resume from last successful company"
    ),
    fail_fast: bool = typer.Option(
        False, "--fail-fast",
        help="Stop on first error instead of continuing"
    ),
    force_refresh: bool = typer.Option(
        True, "--force-refresh/--no-force-refresh",
        help="Force refresh cached data"
    ),
    clear_cache: bool = typer.Option(
        True, "--clear-cache/--no-clear-cache",
        help="Clear HTTP cache before starting"
    ),
    respect_robots: bool = typer.Option(
        True, "--respect-robots/--ignore-robots",
        help="Respect robots.txt"
    ),
    cache_dir: Optional[str] = typer.Option(
        None, "--cache-dir",
        help="HTTP cache directory"
    ),
    requests_per_second: float = typer.Option(
        0.75, "--requests-per-second",
        help="Rate limit for requests"
    ),
    timeout: int = typer.Option(
        15, "--timeout",
        help="Request timeout in seconds"
    ),
):
    """
    Run Wikipedia key people scraping for specified index(es).

    Scrapes key people (executives, board members) from Wikipedia pages
    for companies in the specified index(es). Supports parallel processing
    of multiple indexes with full business metadata extraction.
    """
    try:
        # Import the orchestrator logic directly from the adapters
        from ..adapters.wikipedia_key_people.cli.commands import scrape_index_normalized
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import logging

        # Handle comma-separated index values
        expanded_indexes = []
        for idx in index:
            if ',' in idx:
                expanded_indexes.extend([x.strip() for x in idx.split(',') if x.strip()])
            else:
                expanded_indexes.append(idx)
        index = list(set(expanded_indexes))  # Remove duplicates

        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.INFO)

        typer.echo("🚀 Starting Wikipedia Key People Scraper")
        typer.echo(f"{'='*70}")
        typer.echo(f"Indexes: {', '.join(index)}")
        typer.echo(f"Output directory: {output_dir}")
        typer.echo(f"Workers per index: {workers}")
        if max_companies:
            typer.echo(f"Max companies per index: {max_companies}")
        typer.echo(f"Dry run: {dry_run}")
        typer.echo(f"{'='*70}")

        # Validate indexes
        valid_indexes = {"dow", "sp500", "nasdaq100"}
        invalid_indexes = set(index) - valid_indexes
        if invalid_indexes:
            typer.echo(f"❌ Invalid indexes: {', '.join(invalid_indexes)}", err=True)
            typer.echo(f"Valid options: {', '.join(valid_indexes)}", err=True)
            raise typer.Exit(1)

        if dry_run:
            typer.echo("🔍 DRY RUN - Would execute the following commands:")
            for idx in index:
                typer.echo(f"\n🔍 DRY RUN: Scrape Wikipedia Key People ({idx.upper()})")
                typer.echo(f"   Would extract: companies={extract_companies}, people={extract_people}, roles={extract_roles}, appointments={extract_appointments}")
                typer.echo(f"     index={idx}, output_dir={output_dir}, max_companies={max_companies}")
                typer.echo(f"     workers={workers}, verbose={verbose}, resume={resume}")
                typer.echo(f"     fail_fast={fail_fast}, force_refresh={force_refresh}")
                typer.echo(f"     clear_cache={clear_cache}, respect_robots={respect_robots}")
                typer.echo(f"     cache_dir={cache_dir}, requests_per_second={requests_per_second}")
                typer.echo(f"     timeout={timeout}")
            typer.echo(f"\n{'='*70}")
            return

        # Execute scraping for each index
        success_count = 0
        total_indexes = len(index)

        if total_indexes == 1:
            # Single index - run directly
            try:
                scrape_index_normalized(
                    index=index[0],
                    output_dir=output_dir,
                    max_companies=max_companies,
                    workers=workers,
                    resume=resume,
                    fail_fast=fail_fast,
                    dry_run=False,
                    verbose=verbose,
                    force_refresh=force_refresh,
                    clear_cache=clear_cache,
                    respect_robots=respect_robots,
                    cache_dir=cache_dir,
                    requests_per_second=requests_per_second,
                    timeout=timeout,
                    extract_companies=extract_companies,
                    extract_people=extract_people,
                    extract_roles=extract_roles,
                    extract_appointments=extract_appointments,
                )
                success_count += 1
                typer.echo(f"✅ Scrape Wikipedia Key People ({index[0].upper()}) completed successfully!")
            except Exception as e:
                typer.echo(f"❌ Scrape Wikipedia Key People ({index[0].upper()}) failed: {e}", err=True)
                if fail_fast:
                    raise typer.Exit(1)
        else:
            # Multiple indexes - run in parallel
            typer.echo(f"🔄 Processing {total_indexes} indexes in parallel...")

            with ThreadPoolExecutor(max_workers=min(total_indexes, 3)) as executor:
                futures = {}
                for idx in index:
                    future = executor.submit(
                        scrape_index_normalized,
                        index=idx,
                        output_dir=output_dir,
                        max_companies=max_companies,
                        workers=workers,
                        resume=resume,
                        fail_fast=fail_fast,
                        dry_run=False,
                        verbose=verbose,
                        force_refresh=force_refresh,
                        clear_cache=clear_cache,
                        respect_robots=respect_robots,
                        cache_dir=cache_dir,
                        requests_per_second=requests_per_second,
                        timeout=timeout,
                        extract_companies=extract_companies,
                        extract_people=extract_people,
                        extract_roles=extract_roles,
                        extract_appointments=extract_appointments,
                    )
                    futures[future] = idx

                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        future.result()
                        success_count += 1
                        typer.echo(f"✅ Scrape Wikipedia Key People ({idx.upper()}) completed successfully!")
                    except Exception as e:
                        typer.echo(f"❌ Scrape Wikipedia Key People ({idx.upper()}) failed: {e}", err=True)
                        if fail_fast:
                            raise typer.Exit(1)

        typer.echo(f"\n🎉 Completed {success_count}/{total_indexes} indexes successfully!")

        if success_count == 0:
            typer.echo("❌ No indexes were processed successfully", err=True)
            raise typer.Exit(1)

    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()






