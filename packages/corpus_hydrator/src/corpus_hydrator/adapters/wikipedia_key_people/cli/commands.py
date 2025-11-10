"""
CLI Commands for Wikipedia Key People Scraper

This module provides command-line interfaces for the Wikipedia key people scraper,
following clean architecture principles.
"""

import logging
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
import typer
from corpus_types.schemas.wikipedia_key_people import (
    WikipediaExtractionResult,
    get_default_config,
)

from ..core.scraper import WikipediaKeyPeopleScraper

# Set up logging
logger = logging.getLogger(__name__)

app = typer.Typer(
    name="wikipedia-key-people", help="Extract key people information from Wikipedia"
)


@app.command("scrape-index-normalized")
def scrape_index_normalized(
    index: str = typer.Option(
        "dow", "--index", "-i", help="Index to scrape (dow, sp500, nasdaq100)"
    ),
    output_dir: str = typer.Option(
        "data", "--output-dir", "-o", help="Output directory for normalized tables"
    ),
    max_companies: Optional[int] = typer.Option(
        None, "--max-companies", help="Maximum number of companies to process"
    ),
    workers: int = typer.Option(
        1, "--workers", "-w", help="Number of parallel workers (default: 1)"
    ),
    resume: bool = typer.Option(
        False, "--resume", help="Resume processing from last successful company"
    ),
    fail_fast: bool = typer.Option(
        False, "--fail-fast", help="Stop on first error instead of continuing"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be done without executing"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
    force_refresh: bool = typer.Option(
        False, "--force-refresh", help="Force refresh cached data"
    ),
    clear_cache: bool = typer.Option(
        False, "--clear-cache", help="Clear HTTP cache before starting"
    ),
    respect_robots: bool = typer.Option(
        True,
        "--respect-robots/--ignore-robots",
        help="Respect robots.txt (default: True)",
    ),
    cache_dir: Optional[str] = typer.Option(
        None,
        "--cache-dir",
        help="Directory for HTTP cache (default: ~/.cache/wikipedia_key_people)",
    ),
    requests_per_second: float = typer.Option(
        0.75, "--requests-per-second", help="Rate limit for requests (default: 0.75)"
    ),
    timeout: int = typer.Option(
        15, "--timeout", help="Request timeout in seconds (default: 15)"
    ),
    extract_companies: bool = typer.Option(
        True, "--companies/--no-companies", help="Extract company information"
    ),
    extract_people: bool = typer.Option(
        True, "--people/--no-people", help="Extract key people information"
    ),
    extract_roles: bool = typer.Option(
        True, "--roles/--no-roles", help="Extract role definitions"
    ),
    extract_appointments: bool = typer.Option(
        True, "--appointments/--no-appointments", help="Extract person-company-role relationships"
    ),
):
    """
    Extract key people in production-ready normalized format.

    Creates separate CSV files for companies, people, roles, and appointments
    with proper relationships and governance metadata.
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if dry_run:
        typer.echo(f"DRY RUN: Would extract key people for {index}")
        typer.echo(f"  Output directory: {output_dir}")
        typer.echo(f"  Max companies: {max_companies or 'all'}")
        typer.echo(f"  Workers: {workers}")
        typer.echo(f"  Resume: {resume}")
        typer.echo(f"  Fail fast: {fail_fast}")
        return

    try:
        from ..config import get_default_config
        from ..usecase import WikipediaKeyPeopleUseCase

        # Initialize components
        config = get_default_config()
        usecase = WikipediaKeyPeopleUseCase(config)

        # Run normalized extraction
        result = usecase.extract_index_normalized(
            index_name=index,
            output_dir=output_dir,
            max_companies=max_companies,
            workers=workers,
            extract_companies=extract_companies,
            extract_people=extract_people,
            extract_roles=extract_roles,
            extract_appointments=extract_appointments,
        )

        if "error" in result:
            typer.echo(f"Error: {result['error']}", err=True)
            raise typer.Exit(1)

        # Display results
        typer.echo("Normalized extraction completed!")
        typer.echo(f"Index: {result['index_name']}")
        typer.echo(f"Companies processed: {result['companies_processed']}")
        typer.echo(f"Companies successful: {result['companies_successful']}")
        typer.echo(f"People extracted: {result['people_extracted']}")
        typer.echo(f"Roles identified: {result['roles_identified']}")
        typer.echo(f"Appointments created: {result['appointments_created']}")

        typer.echo("\nOutput files:")
        for file in result["output_files"]:
            typer.echo(f"  • {output_dir}/{file}")

        typer.echo("\nManifest:")
        typer.echo(f"  Schema version: {result['manifest']['schema_version']}")
        typer.echo(f"  License: {result['manifest']['governance']['license']}")

    except Exception as e:
        typer.echo(f"Extraction failed: {e}", err=True)
        if verbose:
            import traceback

            typer.echo(traceback.format_exc(), err=True)
        raise typer.Exit(1)


# REMOVED: scrape_index command - keeping only scrape-index-normalized


# REMOVED: scrape_multiple command - keeping only scrape-index-normalized


# REMOVED: _save_results function - no longer used after removing other commands


if __name__ == "__main__":
    app()
