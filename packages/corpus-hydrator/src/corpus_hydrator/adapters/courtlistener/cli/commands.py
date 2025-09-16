"""
CLI Commands for CourtListener Adapter

This module provides command-line interfaces for CourtListener operations,
following clean architecture principles.
"""

import sys
import logging
from pathlib import Path
from typing import Optional, List

import typer

from ..usecase import CourtListenerUseCase
from ..config import get_default_config
from corpus_types.schemas import CourtListenerConfig
# Only import what's actually used in the simplified CLI
from ..parsers.query_builder import STATUTE_QUERIES

# Set up logging
logger = logging.getLogger(__name__)

app = typer.Typer(
    name="courtlistener",
    help="CourtListener legal data collection and processing"
)


@app.command("orchestrate")
def orchestrate(
    statutes: List[str] = typer.Option(
        None,
        "--statutes",
        "-s",
        help="Statutes to process (default: all supported)",
    ),
    company_file: Optional[Path] = typer.Option(
        None,
        "--company-file",
        "-c",
        exists=True,
        readable=True,
        help="CSV with company names (column 'official_name')",
    ),
    outdir: Path = typer.Option(
        Path("CourtListener"),
        "--outdir",
        "-o",
        help="Base output directory",
    ),
    pages: int = typer.Option(
        1, "--pages", "-p", min=1, help="Pages per search request"
    ),
    page_size: int = typer.Option(
        50, "--page-size", min=1, max=100, help="Results per page"
    ),
    date_min: Optional[str] = typer.Option(
        None, "--date-min", help="Earliest filing date (YYYY-MM-DD)"
    ),
    api_mode: str = typer.Option(
        "standard", "--api-mode", "-m", help="'standard' or 'recap'"
    ),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="CourtListener API token (overrides env/--config)",
    ),
    print_query_chunks: bool = typer.Option(
        False,
        "--print-query-chunks",
        help="Print the number of query chunks and exit (for debugging)",
    ),
    chunk_size: int = typer.Option(
        10,
        "--chunk-size",
        help="Number of companies per query chunk (default: 10)",
    ),
    async_mode: bool = typer.Option(
        False,
        "--async/--no-async",
        help="Run document fetching in parallel using asyncio (default: False)",
    ),
):
    """Run the full multi-step CourtListener workflow in one go. Use --async for parallel doc fetching."""

    if print_query_chunks:
        if not statutes or not company_file:
            print("--print-query-chunks requires --statutes and --company-file")
            sys.exit(1)

        from ..parsers.query_builder import QueryBuilder
        builder = QueryBuilder()

        for statute in statutes:
            queries = builder.build_statute_query(statute, company_file, chunk_size=chunk_size)
            with open(company_file, newline="") as f:
                import csv
                company_count = sum(1 for _ in csv.DictReader(f))

            print(f"Statute: {statute}")
            print(f"  Companies: {company_count}")
            print(f"  Query chunks: {len(queries)} (chunk size: {chunk_size})")
        sys.exit(0)

    config = get_default_config()
    if token:
        config.api_token = token

    try:
        usecase = CourtListenerUseCase(
            config=config,
            statutes=statutes or ["FTC Section 5 (9th Cir.)"],
            company_file=company_file,
            outdir=outdir,
            token=token,
            pages=pages,
            page_size=page_size,
            date_min=date_min,
            api_mode=api_mode,
            chunk_size=chunk_size,
            async_mode=async_mode,
        )

        if async_mode:
            import asyncio
            asyncio.run(usecase.run_async())
        else:
            usecase.run()

    except Exception:
        logger.exception("Fatal error during orchestration")
        raise typer.Exit(code=1)


@app.command("search")
def search(
    statutes: List[str] = typer.Option(
        list(STATUTE_QUERIES.keys()),
        "--statutes",
        "-s",
        help="Statutes to search (default: all)",
    ),
    pages: int = typer.Option(
        None, "--pages", "-p", help="Number of pages to fetch per statute"
    ),
    page_size: int = typer.Option(
        None, "--page-size", help="Results per page (max 100)"
    ),
    date_min: str = typer.Option(
        None, "--date-min", help="Earliest filing date (YYYY-MM-DD)"
    ),
    opinions: bool = typer.Option(False, "--opinions", help="Also fetch opinion texts"),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Output directory (default: data/raw/courtlistener/YYYY-MM-DD)",
    ),
    api_mode: str = typer.Option(
        "standard", "--api-mode", "-m", help="API mode to use (standard or recap)"
    ),
    company_file: str = typer.Option(
        None,
        "--company-file",
        "-c",
        help="CSV of company names (official_name) for filtering",
    ),
    chunk_size: int = typer.Option(
        10,
        "--chunk-size",
        help="Number of companies per query chunk (default: 10)",
    ),
):
    """Run batch searches for specified statutes."""

    config = get_default_config()

    # Validate statutes
    invalid_statutes = [s for s in statutes if s not in STATUTE_QUERIES]
    if invalid_statutes:
        logger.error(f"Unknown statutes: {', '.join(invalid_statutes)}")
        raise typer.Exit(1)

    # Validate API mode
    if api_mode not in ["standard", "recap"]:
        logger.error(f"Invalid API mode: {api_mode}. Must be 'standard' or 'recap'")
        raise typer.Exit(1)

    # Run searches
    try:
        process_statutes(
            statutes=statutes,
            config=config,
            pages=pages or config.default_pages,
            page_size=page_size or config.default_page_size,
            date_min=date_min or config.default_date_min,
            output_dir=output_dir or config.output_dir,
            api_mode=api_mode,
            company_file=company_file,
            chunk_size=chunk_size,
        )
    except Exception as e:
        logger.exception("Error during batch processing")
        raise typer.Exit(1)

# Clean SSOT module - legacy commands removed
