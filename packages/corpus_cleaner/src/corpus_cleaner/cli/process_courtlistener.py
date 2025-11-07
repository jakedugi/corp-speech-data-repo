#!/usr/bin/env python3
"""
CLI interface for processing CourtListener data.

Usage:
    python -m corpus_cleaner.cli.process_courtlistener \
        --input CourtListener/ \
        --output data/courtlistener_normalized.jsonl
"""

import json
import logging
import sys
from pathlib import Path
from typing import List, Optional

import typer

from ..courtlistener_processor import CourtListenerProcessor

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def process(  # type: ignore[no-untyped-def]
    input_dir: Path = typer.Option(
        ..., "--input", help="Input CourtListener directory (e.g., CourtListener/)"
    ),
    output_file: Path = typer.Option(
        ..., "--output", help="Output normalized JSONL file"
    ),
    config_file: Optional[Path] = typer.Option(
        None, "--config", help="Configuration YAML file"
    ),
    include_entries: bool = typer.Option(
        True, "--entries/--no-entries", help="Include entries (docket entries)"
    ),
    include_opinions: bool = typer.Option(
        True, "--opinions/--no-opinions", help="Include opinions"
    ),
    case_filter: Optional[str] = typer.Option(
        None,
        "--cases",
        help="Comma-separated list of case IDs to process (if not provided, process all)",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
    stats_file: Optional[Path] = typer.Option(
        None, "--stats", help="Output statistics to JSON file"
    ),
):
    """
    Process CourtListener data from hydrator output.
    
    Navigates case directories, extracts plain_text from entries and opinions,
    cleans and normalizes the text, and outputs JSONL documents ready for
    the extractor module.
    
    Examples:
    
        # Process all cases
        corpus-clean-courtlistener \\
            --input CourtListener/ \\
            --output data/normalized.jsonl
        
        # Process only opinions from specific cases
        corpus-clean-courtlistener \\
            --input CourtListener/ \\
            --output data/opinions.jsonl \\
            --no-entries \\
            --cases "1:22-cv-10979_nysd,2:23-cv-01495_wawd"
        
        # Process with custom config and save stats
        corpus-clean-courtlistener \\
            --input CourtListener/ \\
            --output data/normalized.jsonl \\
            --config configs/cleaner.yaml \\
            --stats data/stats.json \\
            --verbose
    """
    # Setup logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    logger.info("=" * 70)
    logger.info("CourtListener Data Processor")
    logger.info("=" * 70)
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output file: {output_file}")
    logger.info(f"Include entries: {include_entries}")
    logger.info(f"Include opinions: {include_opinions}")

    # Parse case filter
    case_filter_list = None
    if case_filter:
        case_filter_list = [c.strip() for c in case_filter.split(",")]
        logger.info(f"Processing only cases: {case_filter_list}")

    # Load configuration
    config = {}
    if config_file and config_file.exists():
        import yaml

        with open(config_file) as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_file}")

    # Validate input
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        raise typer.Exit(1)

    if not input_dir.is_dir():
        logger.error(f"Input path is not a directory: {input_dir}")
        raise typer.Exit(1)

    # Initialize processor
    processor = CourtListenerProcessor(config)

    # Process all cases
    try:
        stats = processor.process_all_cases(
            base_dir=input_dir,
            output_file=output_file,
            include_entries=include_entries,
            include_opinions=include_opinions,
            case_filter=case_filter_list,
        )

        # Save statistics if requested
        if stats_file:
            stats_file.parent.mkdir(parents=True, exist_ok=True)
            with open(stats_file, "w") as f:
                json.dump(stats, f, indent=2)
            logger.info(f"Statistics saved to: {stats_file}")

        # Print summary
        logger.info("=" * 70)
        logger.info("Processing Summary")
        logger.info("=" * 70)
        logger.info(f"Cases processed: {stats['total_cases']}")
        logger.info(f"Total documents: {stats['total_documents']}")
        logger.info(f"  - Entries: {stats['entries_count']}")
        logger.info(f"  - Opinions: {stats['opinions_count']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info("=" * 70)
        logger.info("✅ Processing complete!")

        if stats["errors"] > 0:
            logger.warning(f"⚠️  {stats['errors']} errors occurred during processing")
            raise typer.Exit(1)

    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)
        raise typer.Exit(1)


@app.command()
def list_cases(  # type: ignore[no-untyped-def]
    input_dir: Path = typer.Option(
        ..., "--input", help="Input CourtListener directory"
    ),
):
    """
    List all case directories found in the CourtListener data.

    This is useful for discovering what cases are available and getting
    their IDs for selective processing.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        raise typer.Exit(1)

    processor = CourtListenerProcessor()

    try:
        case_dirs = processor.discover_cases(input_dir)

        logger.info("=" * 70)
        logger.info(f"Found {len(case_dirs)} case directories:")
        logger.info("=" * 70)

        for i, case_dir in enumerate(case_dirs, 1):
            # Count documents
            entries = list(processor.extract_entries(case_dir))
            opinions = list(processor.extract_opinions(case_dir))

            logger.info(f"{i:3d}. {case_dir.name}")
            logger.info(f"      Entries: {len(entries)}, Opinions: {len(opinions)}")

        logger.info("=" * 70)
        logger.info(f"Total: {len(case_dirs)} cases")

    except Exception as e:
        logger.error(f"Error listing cases: {e}", exc_info=True)
        raise typer.Exit(1)


@app.command()
def inspect(  # type: ignore[no-untyped-def]
    input_dir: Path = typer.Option(
        ..., "--input", help="Input CourtListener directory"
    ),
    case_id: str = typer.Option(..., "--case", help="Case ID to inspect"),
):
    """
    Inspect a specific case and show its documents.

    This is useful for understanding the structure and content of a case
    before processing.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        raise typer.Exit(1)

    case_dir = input_dir / case_id
    if not case_dir.exists():
        logger.error(f"Case directory not found: {case_dir}")
        raise typer.Exit(1)

    processor = CourtListenerProcessor()

    try:
        logger.info("=" * 70)
        logger.info(f"Case: {case_id}")
        logger.info("=" * 70)

        # Show entries
        entries = list(processor.extract_entries(case_dir))
        logger.info(f"\nEntries: {len(entries)}")
        for i, entry in enumerate(entries, 1):
            doc_data = entry["doc_data"]
            text_len = len(doc_data.get("plain_text", ""))
            logger.info(f"  {i}. ID: {doc_data.get('id')}")
            logger.info(f"     File: {entry['file_path'].name}")
            logger.info(f"     Description: {doc_data.get('description', 'N/A')}")
            logger.info(f"     Text length: {text_len:,} chars")

        # Show opinions
        opinions = list(processor.extract_opinions(case_dir))
        logger.info(f"\nOpinions: {len(opinions)}")
        for i, opinion in enumerate(opinions, 1):
            doc_data = opinion["doc_data"]
            text_len = len(doc_data.get("plain_text", ""))
            logger.info(f"  {i}. ID: {doc_data.get('id')}")
            logger.info(f"     File: {opinion['file_path'].name}")
            logger.info(f"     Type: {doc_data.get('type', 'N/A')}")
            logger.info(f"     Author: {doc_data.get('author_str', 'N/A')}")
            logger.info(f"     Text length: {text_len:,} chars")

        logger.info("=" * 70)
        logger.info(f"Total documents: {len(entries) + len(opinions)}")

    except Exception as e:
        logger.error(f"Error inspecting case: {e}", exc_info=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
