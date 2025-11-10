#!/usr/bin/env python3
"""
CLI Orchestrator for Corporate Speech Data Pipeline - Wikipedia Key People Step

This script runs the Wikipedia key people scraping step with customizable parameters.

Quick Start:
    # Run with defaults (dow index, data output dir)
    python scripts/orchestrator.py run

    # Run multiple indexes in parallel
    python scripts/orchestrator.py run --index dow --index sp500

    # Test with small dataset
    python scripts/orchestrator.py run --max-companies 2 --dry-run

    # Custom settings
    python scripts/orchestrator.py run --index sp500 --output-dir data --workers 4 --verbose

    # Get help
    python scripts/orchestrator.py --help
"""

import logging
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

import typer

# Default configurations for Wikipedia key people step
DEFAULTS = {
    "indexes": ["dow"],  # dow, sp500, nasdaq100
    "output_dir": "data",
    "max_companies": None,  # None means all
    "workers": 4,
    "verbose": True,
    "dry_run": False,
    "resume": False,
    "fail_fast": False,
    "force_refresh": True,
    "clear_cache": True,
    "respect_robots": True,
    "cache_dir": None,
    "requests_per_second": 0.75,
    "timeout": 15,
}

app = typer.Typer(
    name="orchestrator",
    help="Orchestrate Wikipedia key people scraping"
)
logger = logging.getLogger(__name__)


def run_command(cmd: List[str], description: str) -> bool:
    """Run a command and return success status."""
    typer.echo(f"\n{'='*70}")
    typer.echo(f"🔄 {description}")
    typer.echo(f"{'='*70}")
    typer.echo(f"Command: {' '.join(cmd)}")
    typer.echo()

    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        typer.echo(f"✅ {description} completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        typer.echo(f"❌ {description} failed with exit code {e.returncode}", err=True)
        if e.stdout:
            typer.echo(f"STDOUT: {e.stdout.decode()}", err=True)
        if e.stderr:
            typer.echo(f"STDERR: {e.stderr.decode()}", err=True)
        return False
    except Exception as e:
        typer.echo(f"❌ {description} failed: {e}", err=True)
        return False


def run_wikipedia_scrape(index: str, output_dir: str, max_companies: Optional[int],
                        workers: int, verbose: bool, resume: bool, fail_fast: bool,
                        force_refresh: bool, clear_cache: bool, respect_robots: bool,
                        cache_dir: Optional[str], requests_per_second: float, timeout: int,
                        dry_run: bool, extract_companies: bool, extract_people: bool,
                        extract_roles: bool, extract_appointments: bool) -> bool:
    """Run Wikipedia key people scraping for a single index."""
    description = f"Scrape Wikipedia Key People ({index.upper()})"

    # Import and call the function directly to avoid CLI issues
    import sys
    import os
    from pathlib import Path

    # Add the package paths to sys.path
    project_root = Path(__file__).parent.parent
    packages_dir = project_root / "packages"

    for package in ["corpus_hydrator", "corpus_types", "corpus_extractors", "corpus_cleaner"]:
        package_src = packages_dir / package / "src"
        if str(package_src) not in sys.path:
            sys.path.insert(0, str(package_src))

    if dry_run:
        typer.echo(f"\n🔍 DRY RUN: {description}")
        extraction_parts = []
        if extract_companies: extraction_parts.append("companies")
        if extract_people: extraction_parts.append("people")
        if extract_roles: extraction_parts.append("roles")
        if extract_appointments: extraction_parts.append("appointments")
        typer.echo(f"   Would extract: {', '.join(extraction_parts)}")
        typer.echo(f"     index={index}, output_dir={output_dir}, max_companies={max_companies}")
        typer.echo(f"     workers={workers}, verbose={verbose}, resume={resume}")
        typer.echo(f"     fail_fast={fail_fast}, force_refresh={force_refresh}")
        typer.echo(f"     clear_cache={clear_cache}, respect_robots={respect_robots}")
        typer.echo(f"     cache_dir={cache_dir}, requests_per_second={requests_per_second}")
        typer.echo(f"     timeout={timeout}")
        return True

    try:
        # Import the function dynamically
        from corpus_hydrator.adapters.wikipedia_key_people.cli.commands import scrape_index_normalized

        # Call the function directly with the parameters
        # Note: The function signature might not match exactly, so we may need to adjust
        typer.echo(f"\n🔄 {description}")
        typer.echo("=" * 70)

        # Call the function with all parameters, explicitly setting dry_run=False
        result = scrape_index_normalized(
            index=index,
            output_dir=output_dir,
            max_companies=max_companies,
            workers=workers,
            resume=resume,
            fail_fast=fail_fast,
            dry_run=False,  # Explicitly set to False for actual execution
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

        typer.echo(f"✅ {description} completed successfully!")
        return True

    except Exception as e:
        typer.echo(f"❌ {description} failed: {e}", err=True)
        if verbose:
            import traceback
            typer.echo(traceback.format_exc(), err=True)
        return False


@app.command()
def run(
    # Mandatory parameters
    index: List[str] = typer.Option(
        DEFAULTS["indexes"], "--index", "-i",
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
        DEFAULTS["max_companies"], "--max-companies",
        help="Limit number of companies to process (default: all)"
    ),
    workers: int = typer.Option(
        DEFAULTS["workers"], "--workers",
        help="Number of parallel workers"
    ),
    verbose: bool = typer.Option(
        DEFAULTS["verbose"], "--verbose/--no-verbose", "-v",
        help="Enable verbose logging"
    ),
    dry_run: bool = typer.Option(
        DEFAULTS["dry_run"], "--dry-run",
        help="Preview commands without executing"
    ),
    resume: bool = typer.Option(
        DEFAULTS["resume"], "--resume",
        help="Resume from last successful company"
    ),
    fail_fast: bool = typer.Option(
        DEFAULTS["fail_fast"], "--fail-fast",
        help="Stop on first error instead of continuing"
    ),
    force_refresh: bool = typer.Option(
        DEFAULTS["force_refresh"], "--force-refresh/--no-force-refresh",
        help="Force refresh cached data"
    ),
    clear_cache: bool = typer.Option(
        DEFAULTS["clear_cache"], "--clear-cache/--no-clear-cache",
        help="Clear HTTP cache before starting"
    ),
    respect_robots: bool = typer.Option(
        DEFAULTS["respect_robots"], "--respect-robots/--ignore-robots",
        help="Respect robots.txt"
    ),
    cache_dir: Optional[str] = typer.Option(
        DEFAULTS["cache_dir"], "--cache-dir",
        help="HTTP cache directory"
    ),
    requests_per_second: float = typer.Option(
        DEFAULTS["requests_per_second"], "--requests-per-second",
        help="Rate limit for requests"
    ),
    timeout: int = typer.Option(
        DEFAULTS["timeout"], "--timeout",
        help="Request timeout in seconds"
    ),
):
    """
    Run Wikipedia key people scraping for specified index(es).

    Scrapes key people (executives, board members) from Wikipedia pages
    for companies in the specified index(es). Supports parallel processing
    of multiple indexes.
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Handle comma-separated index values
    expanded_indexes = []
    for idx in index:
        if ',' in idx:
            expanded_indexes.extend([x.strip() for x in idx.split(',') if x.strip()])
        else:
            expanded_indexes.append(idx)
    index = list(set(expanded_indexes))  # Remove duplicates

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
        typer.echo(f"❌ Invalid index(es): {', '.join(invalid_indexes)}", err=True)
        typer.echo(f"Valid options: {', '.join(valid_indexes)}", err=True)
        raise typer.Exit(1)

    if len(index) == 0:
        typer.echo("❌ At least one index must be specified", err=True)
        raise typer.Exit(1)

    # If dry run, show what would be executed
    if dry_run:
        typer.echo("🔍 DRY RUN - Would execute the following commands:")
        for idx in index:
            run_wikipedia_scrape(
                index=idx,
                output_dir=output_dir,
                max_companies=max_companies,
                workers=workers,
                verbose=verbose,
                resume=resume,
                fail_fast=fail_fast,
                force_refresh=force_refresh,
                clear_cache=clear_cache,
                respect_robots=respect_robots,
                cache_dir=cache_dir,
                requests_per_second=requests_per_second,
                timeout=timeout,
                dry_run=True,
                extract_companies=extract_companies,
                extract_people=extract_people,
                extract_roles=extract_roles,
                extract_appointments=extract_appointments
            )
        typer.echo(f"\n{'='*70}")
        return

    # Execute scraping for each index
    success_count = 0
    total_indexes = len(index)

    if total_indexes == 1:
        # Single index - run directly
        success = run_wikipedia_scrape(
            index=index[0],
            output_dir=output_dir,
            max_companies=max_companies,
            workers=workers,
            verbose=verbose,
            resume=resume,
            fail_fast=fail_fast,
            force_refresh=force_refresh,
            clear_cache=clear_cache,
            respect_robots=respect_robots,
            cache_dir=cache_dir,
            requests_per_second=requests_per_second,
            timeout=timeout,
            dry_run=False,
            extract_companies=extract_companies,
            extract_people=extract_people,
            extract_roles=extract_roles,
            extract_appointments=extract_appointments
        )
        if success:
            success_count += 1
    else:
        # Multiple indexes - run in parallel
        typer.echo(f"🔄 Processing {total_indexes} indexes in parallel...")

        with ThreadPoolExecutor(max_workers=min(total_indexes, 3)) as executor:
            futures = {}
            for idx in index:
                future = executor.submit(
                    run_wikipedia_scrape,
                    index=idx,
                    output_dir=output_dir,
                    max_companies=max_companies,
                    workers=workers,
                    verbose=verbose,
                    resume=resume,
                    fail_fast=fail_fast,
                    force_refresh=force_refresh,
                    clear_cache=clear_cache,
                    respect_robots=respect_robots,
                    cache_dir=cache_dir,
                    requests_per_second=requests_per_second,
                    timeout=timeout,
                    dry_run=False,
                    extract_companies=extract_companies,
                    extract_people=extract_people,
                    extract_roles=extract_roles,
                    extract_appointments=extract_appointments
                )
                futures[future] = idx

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                        typer.echo(f"✅ {idx.upper()} scraping completed successfully")
                    else:
                        typer.echo(f"❌ {idx.upper()} scraping failed")
                except Exception as e:
                    typer.echo(f"❌ {idx.upper()} scraping failed with exception: {e}", err=True)

    typer.echo(f"\n{'='*70}")
    if success_count == total_indexes:
        typer.echo(f"🎉 All indexes scraped successfully! ({success_count}/{total_indexes})")
    else:
        typer.echo(f"⚠️  Completed with issues ({success_count}/{total_indexes} indexes successful)")
    typer.echo(f"{'='*70}")


@app.command()
def defaults():
    """
    Show default configuration values.
    """
    typer.echo("📋 Default Configuration:")
    typer.echo(f"{'='*70}")
    typer.echo(f"indexes: {', '.join(DEFAULTS['indexes'])}")
    typer.echo(f"output_dir: {DEFAULTS['output_dir']}")
    typer.echo(f"max_companies: {DEFAULTS['max_companies'] or 'all'}")
    typer.echo(f"workers: {DEFAULTS['workers']}")
    typer.echo(f"verbose: {DEFAULTS['verbose']}")
    typer.echo(f"dry_run: {DEFAULTS['dry_run']}")
    typer.echo(f"resume: {DEFAULTS['resume']}")
    typer.echo(f"fail_fast: {DEFAULTS['fail_fast']}")
    typer.echo(f"force_refresh: {DEFAULTS['force_refresh']}")
    typer.echo(f"clear_cache: {DEFAULTS['clear_cache']}")
    typer.echo(f"respect_robots: {DEFAULTS['respect_robots']}")
    typer.echo(f"cache_dir: {DEFAULTS['cache_dir'] or 'auto'}")
    typer.echo(f"requests_per_second: {DEFAULTS['requests_per_second']}")
    typer.echo(f"timeout: {DEFAULTS['timeout']}")
    typer.echo(f"{'='*70}")


@app.command()
def indexes():
    """
    Show supported indexes for scraping.
    """
    typer.echo("📊 Supported Indexes:")
    typer.echo(f"{'='*70}")
    indexes_info = [
        ("dow", "Dow Jones Industrial Average (30 companies)"),
        ("sp500", "S&P 500 (500+ companies)"),
        ("nasdaq100", "Nasdaq 100 (100+ companies)"),
    ]
    for idx, description in indexes_info:
        typer.echo(f"• {idx}: {description}")
    typer.echo(f"{'='*70}")
    typer.echo("Use --index to specify one or more indexes")


if __name__ == "__main__":
    app()
