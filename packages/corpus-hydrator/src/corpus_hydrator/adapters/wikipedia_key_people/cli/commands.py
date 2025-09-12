"""
CLI Commands for Wikipedia Key People Scraper

This module provides command-line interfaces for the Wikipedia key people scraper,
following clean architecture principles.
"""

import sys
import logging
from pathlib import Path
from typing import Optional, List
import pandas as pd

import typer

from ..core.scraper import WikipediaKeyPeopleScraper
from corpus_types.schemas.wikipedia_key_people import get_default_config

# Set up logging
logger = logging.getLogger(__name__)

app = typer.Typer(
    name="wikipedia-key-people",
    help="Extract key people information from Wikipedia"
)


@app.command("scrape-index-normalized")
def scrape_index_normalized(
    index: str = typer.Option(
        "dow",
        "--index",
        "-i",
        help="Index to scrape (dow, sp500, nasdaq100)"
    ),
    output_dir: str = typer.Option(
        "data",
        "--output-dir",
        "-o",
        help="Output directory for normalized tables"
    ),
    max_companies: Optional[int] = typer.Option(
        None,
        "--max-companies",
        help="Maximum number of companies to process"
    ),
    workers: int = typer.Option(
        1,
        "--workers",
        "-w",
        help="Number of parallel workers (default: 1)"
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        help="Resume processing from last successful company"
    ),
    fail_fast: bool = typer.Option(
        False,
        "--fail-fast",
        help="Stop on first error instead of continuing"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be done without executing"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging"
    ),
    force_refresh: bool = typer.Option(
        False,
        "--force-refresh",
        help="Force refresh cached data"
    ),
    clear_cache: bool = typer.Option(
        False,
        "--clear-cache",
        help="Clear HTTP cache before starting"
    ),
    respect_robots: bool = typer.Option(
        True,
        "--respect-robots/--ignore-robots",
        help="Respect robots.txt (default: True)"
    ),
    cache_dir: Optional[str] = typer.Option(
        None,
        "--cache-dir",
        help="Directory for HTTP cache (default: ~/.cache/wikipedia_key_people)"
    ),
    requests_per_second: float = typer.Option(
        0.75,
        "--requests-per-second",
        help="Rate limit for requests (default: 0.75)"
    ),
    timeout: int = typer.Option(
        15,
        "--timeout",
        help="Request timeout in seconds (default: 15)"
    )
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
        from ..usecase import WikipediaKeyPeopleUseCase
        from ..config import get_default_config

        # Initialize components
        config = get_default_config()
        usecase = WikipediaKeyPeopleUseCase(config)

        # Run normalized extraction
        result = usecase.extract_index_normalized(
            index_name=index,
            output_dir=output_dir,
            max_companies=max_companies,
            workers=workers
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
        for file in result['output_files']:
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


@app.command()
def scrape_index(
    index: str = typer.Option(
        "sp500", "--index", "-i",
        help="Market index to scrape (sp500, dow, nasdaq100)"
    ),
    output_dir: Path = typer.Option(
        Path("data"), "--output-dir", "-o",
        help="Output directory for scraped data"
    ),
    max_companies: Optional[int] = typer.Option(
        None, "--max-companies", "-m",
        help="Maximum companies to scrape (default: all)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Run without making HTTP requests (for testing)"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable verbose logging"
    ),
):
    """
    Scrape key people information from a Wikipedia market index.

    This command extracts company links from the specified index page,
    then scrapes key people information from each company's Wikipedia page.

    Examples:
        # Scrape S&P 500 key people
        python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index --index sp500

        # Scrape Dow Jones with limit
        python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index --index dow --max-companies 10

        # Dry run for testing
        python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index --index sp500 --dry-run --verbose
    """
    try:
        # Configure scraper
        config = get_default_config()
        config.enabled_indices = [index]
        config.dry_run = dry_run
        config.verbose = verbose

        if max_companies:
            for idx_config in config.indices.values():
                idx_config.max_companies = max_companies

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create and run scraper
        scraper = WikipediaKeyPeopleScraper(config)

        print("Wikipedia Key People Scraper")
        print("=" * 50)
        print(f"Index: {index}")
        print(f"Output: {output_dir}")
        print(f"Dry run: {dry_run}")
        if max_companies:
            print(f"Company limit: {max_companies}")
        print()

        # Run extraction
        result = scraper.scrape_index(index)

        if not result.success:
            print(f"Extraction failed: {result.error_message}")
            sys.exit(1)

        # Save results
        _save_results(result, output_dir)

        # Print summary
        print("\nExtraction Complete!")
        print(f"Companies processed: {result.companies_processed}")
        print(f"Companies successful: {result.companies_successful}")
        print(f"Total key people: {result.total_key_people}")
        print(f"Average key people per company: {result.total_key_people/result.companies_successful:.1f}" if result.companies_successful > 0 else "No successful companies")
        # Show top companies by key people count
        company_counts = {}
        for company in result.companies:
            if company.key_people_count > 0:
                company_counts[f"{company.ticker} ({company.company_name})"] = company.key_people_count

        if company_counts:
            print("\nTop companies by key people count:")
            for company_name, count in sorted(company_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  • {company_name}: {count} people")

        print(f"\nResults saved to: {output_dir}")

    except Exception as e:
        logger.error(f"Error: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@app.command()
def scrape_multiple(
    indices: List[str] = typer.Option(
        ["sp500"], "--indices", "-i",
        help="Market indices to scrape"
    ),
    output_dir: Path = typer.Option(
        Path("data"), "--output-dir", "-o",
        help="Output directory for scraped data"
    ),
    max_companies: Optional[int] = typer.Option(
        None, "--max-companies", "-m",
        help="Maximum companies to scrape per index"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Run without making HTTP requests"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable verbose logging"
    ),
):
    """
    Scrape key people information from multiple Wikipedia market indices.

    Examples:
        # Scrape S&P 500 and Dow Jones
        python -m corpus_hydrator.cli.commands scrape-multiple --indices sp500 dow

        # Scrape all available indices
        python -m corpus_hydrator.cli.commands scrape-multiple --indices sp500 dow nasdaq100
    """
    try:
        # Configure scraper
        config = get_default_config()
        config.enabled_indices = indices
        config.dry_run = dry_run
        config.verbose = verbose

        if max_companies:
            for idx_config in config.indices.values():
                idx_config.max_companies = max_companies

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create and run scraper
        scraper = WikipediaKeyPeopleScraper(config)

        print("Wikipedia Key People Scraper - Multiple Indices")
        print("=" * 60)
        print(f"Indices: {', '.join(indices)}")
        print(f"Output: {output_dir}")
        print(f"Dry run: {dry_run}")
        if max_companies:
            print(f"Company limit per index: {max_companies}")
        print()

        # Run extraction
        results = scraper.scrape_multiple_indices(indices)

        # Process and save results for each index
        total_companies = 0
        total_successful = 0
        total_people = 0

        for index_name, result in results.items():
            if result.success:
                _save_results(result, output_dir / index_name)
                total_companies += result.companies_processed
                total_successful += result.companies_successful
                total_people += result.total_key_people

                print(f"{index_name}: {result.companies_successful}/{result.companies_processed} companies, {result.total_key_people} people")
            else:
                print(f"{index_name}: Failed - {result.error_message}")

        print("\nAll Extractions Complete!")
        print(f"Total companies processed: {total_companies}")
        print(f"Total companies successful: {total_successful}")
        print(f"Total key people extracted: {total_people}")
        print(f"Average key people per company: {total_people/total_successful:.1f}" if total_successful > 0 else "No successful companies")
        print(f"\nResults saved to: {output_dir}")

    except Exception as e:
        logger.error(f"Error: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def _save_results(result: 'WikipediaExtractionResult', output_dir: Path):
    """Save extraction results to files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save key people data
    if result.key_people:
        people_data = []
        for person in result.key_people:
            people_data.append({
                'ticker': person.ticker,
                'company_name': person.company_name,
                'raw_name': person.raw_name,
                'clean_name': person.clean_name,
                'clean_title': person.clean_title,
                'source': person.source,
                'wikipedia_url': person.wikipedia_url,
                'extraction_method': person.extraction_method,
                'scraped_at': person.scraped_at.isoformat() if person.scraped_at else None,
                'parse_success': person.parse_success,
                'confidence_score': person.confidence_score
            })

        df_people = pd.DataFrame(people_data)
        people_file = output_dir / f"{result.index_name}_key_people.csv"
        df_people.to_csv(people_file, index=False)

    # Save company summary
    if result.companies:
        company_data = []
        for company in result.companies:
            company_data.append({
                'ticker': company.ticker,
                'company_name': company.company_name,
                'wikipedia_url': company.wikipedia_url,
                'index_name': company.index_name,
                'key_people_count': company.key_people_count,
                'processing_success': company.processing_success,
                'processed_at': company.processed_at.isoformat() if company.processed_at else None
            })

        df_companies = pd.DataFrame(company_data)
        companies_file = output_dir / f"{result.index_name}_companies.csv"
        df_companies.to_csv(companies_file, index=False)

    # Save statistics
    stats_file = output_dir / f"{result.index_name}_stats.txt"
    with open(stats_file, 'w') as f:
        f.write(f"=== {result.index_name.upper()} Key People Extraction Statistics ===\n\n")
        f.write(f"Operation ID: {result.operation_id}\n")
        f.write(f"Started: {result.started_at}\n")
        f.write(f"Completed: {result.completed_at}\n")
        f.write(f"Duration: {(result.completed_at - result.started_at).total_seconds():.1f} seconds\n\n")

        f.write(f"Companies processed: {result.companies_processed}\n")
        f.write(f"Companies successful: {result.companies_successful}\n")
        f.write(f"Success rate: {result.success_rate:.1%}\n")
        f.write(f"Total key people: {result.total_key_people}\n")

        if result.companies_successful > 0:
            f.write(f"Average key people per company: {result.total_key_people/result.companies_successful:.1f}\n")
        f.write(f"Status: {'SUCCESS' if result.success else 'FAILED'}\n")

        if result.error_message:
            f.write(f"Error: {result.error_message}\n")


if __name__ == "__main__":
    app()
