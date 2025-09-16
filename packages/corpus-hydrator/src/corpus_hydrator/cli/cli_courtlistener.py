"""
Command-line interface for CourtListener batch processing.

This CLI exposes a single orchestrate command for the modern workflow, and a legacy command for the original 7-step process.
"""

import os
from pathlib import Path
from typing import List, Optional
import argparse
import sys

import typer
from loguru import logger
import httpx
from dotenv import load_dotenv

# Import from the new unified CourtListener adapter
from ..adapters.courtlistener.parsers.query_builder import STATUTE_QUERIES
from ..adapters.courtlistener.config import get_default_config
from ..adapters.courtlistener.usecase import CourtListenerUseCase
# Processor functions removed - simplified CLI only uses orchestrate and search

# Legacy workflow imports (commented out until fixed)
# from ..workflows.legacy_multistep import LegacyCourtListenerWorkflow
# from ..shared.logging_utils import setup_logging

load_dotenv()  # Load environment variables from .env if present

# Set up logging to logs/pipeline.log with rotation and INFO level
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)

app = typer.Typer(help="CourtListener batch search CLI")

RESOURCE_FIELDS = {
    "opinions": [
        "id",
        "caseName",
        "dateFiled",
        "snippet",
        "plain_text",
        "html",
        "html_lawbox",
        "cluster",
        "citations",
        "court",
        "judges",
        "type",
        "absolute_url",
    ],
    "dockets": ["id", "caseName", "court", "dateFiled", "docketNumber", "absolute_url"],
    "docket_entries": [
        "id",
        "docket",
        "date_filed",
        "entry_number",
        "description",
        "recap_documents",
    ],
    "recap_docs": [
        "id",
        "docket_entry",
        "plain_text",
        "filepath_local",
        "ocr_status",
        "is_available",
        "date_created",
        "date_modified",
    ],
    "recap": [
        "id",
        "status",
        "created",
        "modified",
        "request_type",
        "docket",
        "recap_document",
    ],
}


@app.command()
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
    outdir: Optional[Path] = typer.Option(
        None,
        "--outdir",
        "-o",
        help="Base output directory (auto-generated for test mode)",
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
    max_companies: Optional[int] = typer.Option(
        None,
        "--max-companies",
        help="Limit number of companies to process for quick testing (default: all)",
    ),
    max_results: Optional[int] = typer.Option(
        None,
        "--max-results",
        help="Limit total results per query for quick testing (default: no limit)",
    ),
    test_mode: bool = typer.Option(
        False,
        "--test-mode",
        help="Test mode: Sets max-cases to 2 for testing",
    ),
    max_cases: int = typer.Option(
        None,
        "--max-cases",
        help="Maximum total cases to process and fully hydrate (overrides all other limits)",
    ),
):
    """Run the CourtListener workflow: search → hydrate dockets with all related data.

    MODES:
    - Base mode: Processes all companies from index with full pagination
    - Test mode (--test-mode): Auto-creates timestamped directory, max 2 cases
    - Custom limit (--max-cases=N): Processes exactly N cases total

    FEATURES:
    - Test mode auto-creates output directory (e.g., test-courtlistener_20240915_143022)
    - Creates directories only when data is actually found
    - 429 errors skip immediately without retries
    - Parallel processing for maximum speed
    - Strict case limits override all other settings

    PDF DOWNLOADS:
    - PDFs require PACER credentials (set PACER_USER and PACER_PASS)
    - Most RECAP documents are behind paywalls and not freely available
    - Disable PDF downloads: export COURTLISTENER_DISABLE_PDF_DOWNLOADS=true
    """
    from ..adapters.courtlistener.parsers.query_builder import build_queries

    if print_query_chunks:
        if not statutes or not company_file:
            print("--print-query-chunks requires --statutes and --company-file")
            sys.exit(1)
        for statute in statutes:
            queries = build_queries(statute, company_file, chunk_size=chunk_size)
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

    # Handle test mode output directory
    if test_mode and not outdir:
        # Auto-generate test output directory with timestamp
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        outdir = f"test-courtlistener_{timestamp}"
        logger.info(f"🧪 TEST MODE: Auto-created output directory: {outdir}")

    # Handle test mode
    if test_mode and max_cases is None:
        max_cases = 2  # Test mode defaults to 2 cases

    # Configuration based on mode
    if max_cases is not None and max_cases <= 5:
        # Strict case limit mode (test mode or manual override)
        max_companies = 1
        max_results = max_cases
        page_size = max_cases
        pages = 1
        if test_mode:
            logger.info(f"🧪 TEST MODE: Max {max_cases} cases in {outdir}")
        else:
            logger.info(f"🎯 STRICT LIMIT MODE: Max {max_cases} total cases")
    elif test_mode:
        # Test mode without strict limit
        max_companies = 1
        max_results = 10  # Allow more results per query
        page_size = 10
        pages = 1
        logger.info(f"🧪 TEST MODE: 1 company, 1 page in {outdir}")
    else:
        # Base mode: All companies with full pagination
        max_companies = None  # Process all companies
        max_results = None    # No limit on results per query
        page_size = 50        # Standard page size for production
        pages = None          # Let pagination handle all pages
        logger.info("📊 BASE MODE: All companies with full pagination")

    try:
        usecase = CourtListenerUseCase(
            config=config,
            statutes=statutes or ["FTC Section 5"],
            company_file=company_file,
            outdir=str(outdir) if outdir else "CourtListener",
            token=token,
            pages=pages,
            page_size=page_size,
            date_min=date_min,
            api_mode=api_mode,
            chunk_size=1 if test_mode else 10,  # 1 company per chunk for test, 10 for base
            max_companies=max_companies,
            max_results=max_results,
            max_cases=max_cases,
        )
        usecase.run()
    except Exception:
        logger.exception("Fatal error during orchestration")
        raise typer.Exit(code=1)


@app.command()
def legacy(
    query: List[str] = typer.Argument(..., help="One or more raw search strings"),
    court: str = typer.Option(None, "--court", "-c", help="Override court id"),
    outdir: Optional[Path] = typer.Option(None, "--outdir", "-o"),
):
    """
    Run the original 7-step multi-call workflow verbatim.
    Note: Legacy workflow not currently implemented in unified structure.
    """
    logger.warning("Legacy workflow not currently implemented in unified structure.")
    logger.info("Use the 'orchestrate' command for the new unified workflow.")
    raise typer.Exit(1)


@app.command()
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
        50,
        "--chunk-size",
        help="Number of companies per query chunk (default: 50)",
    ),
):
    """Run batch searches for specified statutes."""
    # Load configuration
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


def main():
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()


