#!/usr/bin/env python3
"""
CLI interface for corpus-extractors module.

Usage:
    python -m corpus_extractors.cli.extract quotes --input docs.norm.jsonl --output quotes.jsonl
    python -m corpus_extractors.cli.extract outcomes --input docs.norm.jsonl --output outcomes.jsonl
"""

import json
from pathlib import Path
from typing import Optional
import typer
import logging

from ..extraction_pipeline.quote_extractor import QuoteExtractor
from ..extraction_pipeline.extract_outcomes import CaseOutcomeImputer

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def quotes(
    input_file: Path = typer.Option(
        ..., "--input", help="Input normalized documents JSONL file"
    ),
    output_file: Path = typer.Option(..., "--output", help="Output quotes JSONL file"),
    config_file: Optional[Path] = typer.Option(
        None, "--config", help="Quote extraction configuration YAML"
    ),
):
    """
    Extract quotes from normalized documents.

    Processes documents to identify quoted speech, extract spans, and perform
    attribution to speakers where possible.
    """
    logger.info(f"Extracting quotes from {input_file}")
    logger.info(f"Output: {output_file}")

    # Load configuration
    config = {}
    if config_file and config_file.exists():
        file_extension = config_file.suffix.lower()

        if file_extension == '.py':
            # Python config file
            import sys
            config_dir = str(config_file.parent)
            config_name = config_file.stem

            if config_dir not in sys.path:
                sys.path.insert(0, config_dir)

            try:
                config_module = __import__(config_name)
                config = getattr(config_module, 'config', {})
                logger.info(f"Loaded Python config from {config_file}")
            except Exception as e:
                logger.error(f"Failed to load Python config {config_file}: {e}")
                config = {}

        elif file_extension in ['.yaml', '.yml']:
            # YAML config file
            try:
                import yaml
                with open(config_file) as f:
                    config = yaml.safe_load(f)
                logger.info(f"Loaded YAML config from {config_file}")
            except Exception as e:
                logger.error(f"Failed to load YAML config {config_file}: {e}")
                config = {}

        else:
            logger.warning(f"Unsupported config file extension: {file_extension}")
            config = {}

    # Initialize extractor
    extractor = QuoteExtractor(config)

    # Process documents
    quotes = []
    with open(input_file, "r") as infile:
        for i, line in enumerate(infile):
            if i % 100 == 0:
                logger.info(f"Processed {i} documents")

            doc = json.loads(line.strip())
            doc_id = doc.get("doc_id", f"doc_{i}")
            raw_text = doc.get("raw_text", "")
            
            if not raw_text:
                logger.warning(f"Document {doc_id} has no raw_text, skipping")
                continue
            
            # Extract quotes from raw text
            doc_quotes = extractor.extract_quotes(raw_text)
            
            # Convert QuoteCandidate to dict and add doc_id
            for quote in doc_quotes:
                quote_dict = quote.to_dict() if hasattr(quote, 'to_dict') else {
                    "text": quote.quote,
                    "speaker": quote.speaker,
                    "score": quote.score,
                    "context": quote.context,
                    "urls": quote.urls
                }
                quote_dict["doc_id"] = doc_id
                quotes.append(quote_dict)

    # Write output
    with open(output_file, "w") as f:
        for quote in quotes:
            f.write(json.dumps(quote, ensure_ascii=False) + "\n")

    logger.info(f"Successfully extracted {len(quotes)} quotes")


@app.command()
def outcomes(
    input_file: Path = typer.Option(
        ..., "--input", help="Input normalized documents JSONL file"
    ),
    output_file: Path = typer.Option(
        ..., "--output", help="Output outcomes JSONL file"
    ),
    config_file: Optional[Path] = typer.Option(
        None, "--config", help="Outcome extraction configuration YAML"
    ),
):
    """
    Extract case outcomes from normalized documents.

    Parses legal documents to identify case outcomes, labels, and metadata
    including cash amounts, settlement terms, and case dispositions.
    """
    logger.info(f"Extracting outcomes from {input_file}")
    logger.info(f"Output: {output_file}")

    # Load configuration
    config = {}
    if config_file and config_file.exists():
        file_extension = config_file.suffix.lower()

        if file_extension == '.py':
            # Python config file
            import sys
            config_dir = str(config_file.parent)
            config_name = config_file.stem

            if config_dir not in sys.path:
                sys.path.insert(0, config_dir)

            try:
                config_module = __import__(config_name)
                config = getattr(config_module, 'config', {})
                logger.info(f"Loaded Python config from {config_file}")
            except Exception as e:
                logger.error(f"Failed to load Python config {config_file}: {e}")
                config = {}

        elif file_extension in ['.yaml', '.yml']:
            # YAML config file
            try:
                import yaml
                with open(config_file) as f:
                    config = yaml.safe_load(f)
                logger.info(f"Loaded YAML config from {config_file}")
            except Exception as e:
                logger.error(f"Failed to load YAML config {config_file}: {e}")
                config = {}

        else:
            logger.warning(f"Unsupported config file extension: {file_extension}")
            config = {}

    # Initialize extractor
    extractor = CaseOutcomeImputer(config)

    # Process documents
    outcomes = []
    with open(input_file, "r") as infile:
        for i, line in enumerate(infile):
            if i % 1000 == 0:
                logger.info(f"Processed {i} documents")

            doc = json.loads(line.strip())
            doc_outcomes = extractor.extract_outcomes(doc)
            outcomes.extend(doc_outcomes)

    # Write output
    with open(output_file, "w") as f:
        for outcome in outcomes:
            f.write(json.dumps(outcome, ensure_ascii=False) + "\n")

    logger.info(f"Successfully extracted {len(outcomes)} outcomes")


@app.command()
def cash_amounts(
    input_file: Path = typer.Option(..., "--input", help="Input documents JSONL file"),
    output_file: Path = typer.Option(
        ..., "--output", help="Output with extracted cash amounts JSONL file"
    ),
    config_file: Optional[Path] = typer.Option(
        None, "--config", help="Cash extraction configuration YAML"
    ),
):
    """
    Extract cash amounts from documents.

    Specialized extraction for monetary amounts, penalties, settlements,
    and financial figures mentioned in legal documents.
    """
    logger.info(f"Extracting cash amounts from {input_file}")
    logger.info(f"Output: {output_file}")

    # Load configuration
    config = {}
    if config_file and config_file.exists():
        file_extension = config_file.suffix.lower()

        if file_extension == '.py':
            import sys
            config_dir = str(config_file.parent)
            config_name = config_file.stem

            if config_dir not in sys.path:
                sys.path.insert(0, config_dir)

            try:
                config_module = __import__(config_name)
                config = getattr(config_module, 'config', {})
                logger.info(f"Loaded Python config from {config_file}")
            except Exception as e:
                logger.error(f"Failed to load Python config {config_file}: {e}")
                config = {}

        elif file_extension in ['.yaml', '.yml']:
            try:
                import yaml
                with open(config_file) as f:
                    config = yaml.safe_load(f)
                logger.info(f"Loaded YAML config from {config_file}")
            except Exception as e:
                logger.error(f"Failed to load YAML config {config_file}: {e}")
                config = {}

        else:
            logger.warning(f"Unsupported config file extension: {file_extension}")
            config = {}

    # Initialize extractor
    extractor = CaseOutcomeImputer(config)

    # Process documents
    cash_amounts = []
    with open(input_file, "r") as infile:
        for i, line in enumerate(infile):
            if i % 100 == 0:
                logger.info(f"Processed {i} documents")

            doc = json.loads(line.strip())
            doc_id = doc.get("doc_id", f"doc_{i}")
            raw_text = doc.get("raw_text", "")

            if not raw_text:
                logger.warning(f"Document {doc_id} has no raw_text, skipping")
                continue

            # Extract cash amounts from raw text
            doc_amounts = extractor.extract_cash_amounts(raw_text, doc_id)

            # Add doc_id to each amount
            for amount in doc_amounts:
                amount["doc_id"] = doc_id
                cash_amounts.append(amount)

    # Write output
    with open(output_file, "w") as f:
        for amount in cash_amounts:
            f.write(json.dumps(amount, ensure_ascii=False) + "\n")

    logger.info(f"Successfully extracted {len(cash_amounts)} cash amounts")


@app.command()
def combine(
    input_file: Path = typer.Option(
        ..., "--input", help="Input normalized documents JSONL file"
    ),
    quotes_output: Path = typer.Option(
        ..., "--quotes-output", help="Output quotes JSONL file"
    ),
    outcomes_output: Path = typer.Option(
        ..., "--outcomes-output", help="Output outcomes JSONL file"
    ),
    cash_amounts_output: Path = typer.Option(
        ..., "--cash-amounts-output", help="Output cash amounts JSONL file"
    ),
    combined_output: Path = typer.Option(
        ..., "--combined-output", help="Output combined quotes with outcomes JSONL file"
    ),
):
    """
    Run complete end-to-end extraction pipeline and combine results.

    Extracts quotes, outcomes, and cash amounts from documents, then combines
    ALL quotes with case outcomes. Assigns case values using priority logic:
    stipulated_judgment amount (if any) > highest voted cash amount > N/A.
    All quotes from the same case get the same case value.
    """
    logger.info(f"Running complete extraction pipeline on {input_file}")

    # Step 1: Extract quotes
    logger.info("Step 1: Extracting quotes...")
    quotes_extractor = QuoteExtractor()
    quotes = []
    with open(input_file, "r") as infile:
        for i, line in enumerate(infile):
            if i % 100 == 0:
                logger.info(f"Processed {i} documents for quotes")

            doc = json.loads(line.strip())
            doc_id = doc.get("doc_id", f"doc_{i}")
            raw_text = doc.get("raw_text", "")

            if not raw_text:
                continue

            doc_quotes = quotes_extractor.extract_quotes(raw_text)
            for quote in doc_quotes:
                quote_dict = quote.to_dict() if hasattr(quote, 'to_dict') else {
                    "text": quote.quote,
                    "speaker": quote.speaker,
                    "score": quote.score,
                    "context": quote.context,
                    "urls": quote.urls
                }
                quote_dict["doc_id"] = doc_id
                quotes.append(quote_dict)

    with open(quotes_output, "w") as f:
        for quote in quotes:
            f.write(json.dumps(quote, ensure_ascii=False) + "\n")
    logger.info(f"Extracted {len(quotes)} quotes to {quotes_output}")

    # Step 2: Extract outcomes
    logger.info("Step 2: Extracting outcomes...")
    outcomes_extractor = CaseOutcomeImputer()
    outcomes = []
    with open(input_file, "r") as infile:
        for i, line in enumerate(infile):
            if i % 1000 == 0:
                logger.info(f"Processed {i} documents for outcomes")

            doc = json.loads(line.strip())
            doc_outcomes = outcomes_extractor.extract_outcomes(doc)
            outcomes.extend(doc_outcomes)

    with open(outcomes_output, "w") as f:
        for outcome in outcomes:
            f.write(json.dumps(outcome, ensure_ascii=False) + "\n")
    logger.info(f"Extracted {len(outcomes)} outcomes to {outcomes_output}")

    # Step 3: Extract cash amounts
    logger.info("Step 3: Extracting cash amounts...")
    cash_amounts = []
    with open(input_file, "r") as infile:
        for i, line in enumerate(infile):
            if i % 100 == 0:
                logger.info(f"Processed {i} documents for cash amounts")

            doc = json.loads(line.strip())
            doc_id = doc.get("doc_id", f"doc_{i}")
            raw_text = doc.get("raw_text", "")

            if not raw_text:
                continue

            doc_amounts = outcomes_extractor.extract_cash_amounts(raw_text, doc_id)
            for amount in doc_amounts:
                amount["doc_id"] = doc_id
                cash_amounts.append(amount)

    with open(cash_amounts_output, "w") as f:
        for amount in cash_amounts:
            f.write(json.dumps(amount, ensure_ascii=False) + "\n")
    logger.info(f"Extracted {len(cash_amounts)} cash amounts to {cash_amounts_output}")

    # Step 4: Combine quotes with outcomes
    logger.info("Step 4: Combining quotes with outcomes...")
    combine_quotes_with_outcomes(
        quotes_output,
        outcomes_output,
        cash_amounts_output,
        combined_output
    )
    logger.info(f"Combined quotes with outcomes to {combined_output}")

    logger.info("Complete extraction pipeline finished!")




if __name__ == "__main__":
    app()
