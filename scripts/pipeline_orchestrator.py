#!/usr/bin/env python3
"""
Comprehensive Pipeline Orchestrator for Corporate Speech Data Repository

This orchestrator provides end-to-end automation of the entire data pipeline:
1. Data Ingestion (hydrator)
2. Data Cleaning (cleaner)
3. Information Extraction (extractors)

Usage:
    # Run complete pipeline with defaults
    uv run python scripts/pipeline_orchestrator.py run

    # Run with custom configuration
    uv run python scripts/pipeline_orchestrator.py run --config my_config.yaml

    # Run specific stages
    uv run python scripts/pipeline_orchestrator.py run --stages hydrator,cleaner

    # Dry run to see what would be executed
    uv run python scripts/pipeline_orchestrator.py run --dry-run
"""

import json
import logging
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# Add local packages to path
project_root = Path(__file__).parent.parent
packages_dir = project_root / "packages"
for package_dir in ["corpus_hydrator/src", "corpus_cleaner/src", "corpus_extractors/src", "corpus_types/src"]:
    full_path = packages_dir / package_dir
    if str(full_path) not in sys.path:
        sys.path.insert(0, str(full_path))

try:
    import typer
    import yaml
except ImportError as e:
    print(f"Error: Required dependencies not found. Please run with uv: uv run python scripts/pipeline_orchestrator.py")
    print(f"Missing: {e}")
    sys.exit(1)

app = typer.Typer()
logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Complete configuration for the pipeline orchestrator."""

    # Global settings
    output_dir: Path = Path("data")
    temp_dir: Path = Path("data/temp")
    log_level: str = "INFO"
    dry_run: bool = False
    force: bool = False

    # Pipeline stages to run
    stages: Set[str] = field(default_factory=lambda: {"hydrator", "cleaner", "extractors"})

    # Hydrator configuration
    hydrator: Dict[str, Any] = field(default_factory=dict)

    # Cleaner configuration
    cleaner: Dict[str, Any] = field(default_factory=dict)

    # Extractors configuration
    extractors: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def get_defaults(cls) -> 'PipelineConfig':
        """Get sensible default configuration."""
        return cls(
            hydrator={
                "sources": ["courtlistener", "wikipedia"],
                "courtlistener": {
                    "query": {
                        "date_range": {
                            "start": "2020-01-01",
                            "end": "2023-12-31"
                        },
                        "courts": ["scotus", "ca1", "ca2", "ca9"],
                        "case_types": ["civil"],
                        "keywords": ["corporate", "securities", "antitrust", "environmental"],
                        "max_results": 1000
                    }
                },
                "wikipedia": {
                    "indices": ["sp500"],
                    "max_companies": 50,
                    "dry_run": False
                },
                "index_constituents": {
                    "indices": ["sp500"],
                    "formats": ["csv", "parquet"]
                }
            },
            cleaner={
                "text_cleaning": {
                    "remove_extra_whitespace": True,
                    "normalize_unicode": True,
                    "preserve_case": True,
                    "remove_page_breaks": True
                },
                "output_format": "jsonl",
                "include_metadata": True
            },
            extractors={
                "quotes": {
                    "enabled": True,
                    "min_quote_length": 10,
                    "max_quote_length": 1000,
                    "speaker_attribution": True,
                    "confidence_threshold": 0.5
                },
                "outcomes": {
                    "enabled": True,
                    "extract_amounts": True,
                    "case_types": ["settlement", "judgment", "dismissal"]
                },
                "cash_amounts": {
                    "enabled": True,
                    "min_amount": 1000,
                    "currency_normalization": True
                },
                "combine_results": True
            }
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        result = {
            "output_dir": str(self.output_dir),
            "temp_dir": str(self.temp_dir),
            "log_level": self.log_level,
            "dry_run": self.dry_run,
            "force": self.force,
            "stages": list(self.stages),
            "hydrator": self.hydrator,
            "cleaner": self.cleaner,
            "extractors": self.extractors
        }
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PipelineConfig':
        """Create from dictionary (e.g., loaded from YAML)."""
        # Merge with defaults
        defaults = cls.get_defaults()

        config = cls()
        config.output_dir = Path(data.get("output_dir", defaults.output_dir))
        config.temp_dir = Path(data.get("temp_dir", defaults.temp_dir))
        config.log_level = data.get("log_level", defaults.log_level)
        config.dry_run = data.get("dry_run", defaults.dry_run)
        config.force = data.get("force", defaults.force)
        config.stages = set(data.get("stages", defaults.stages))

        # Deep merge configurations
        config.hydrator = cls._deep_merge(defaults.hydrator, data.get("hydrator", {}))
        config.cleaner = cls._deep_merge(defaults.cleaner, data.get("cleaner", {}))
        config.extractors = cls._deep_merge(defaults.extractors, data.get("extractors", {}))

        return config

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = PipelineConfig._deep_merge(result[key], value)
            else:
                result[key] = value
        return result


class PipelineOrchestrator:
    """Orchestrates the complete data pipeline."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._setup_logging()

    def _setup_logging(self):
        """Setup logging configuration."""
        level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def run_pipeline(self, is_dry_run: bool = False) -> bool:
        """Run the complete pipeline."""
        if not is_dry_run:
            logger.info("🚀 Starting Corporate Speech Data Pipeline")
            logger.info(f"Output directory: {self.config.output_dir}")
            logger.info(f"Stages to run: {', '.join(sorted(self.config.stages))}")

        if self.config.dry_run:
            logger.info("🔍 DRY RUN MODE - No actual execution")
            return self._dry_run()

        # Ensure directories exist
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.config.temp_dir.mkdir(parents=True, exist_ok=True)

        success = True

        try:
            # Stage 1: Data Hydration
            if "hydrator" in self.config.stages:
                if not self._run_hydrator():
                    success = False
                    if not self.config.force:
                        return False

            # Stage 2: Data Cleaning
            if "cleaner" in self.config.stages:
                if not self._run_cleaner():
                    success = False
                    if not self.config.force:
                        return False

            # Stage 3: Information Extraction
            if "extractors" in self.config.stages:
                if not self._run_extractors():
                    success = False
                    if not self.config.force:
                        return False

            if success:
                logger.info("✅ Pipeline completed successfully!")
                self._generate_summary()
            else:
                logger.error("❌ Pipeline completed with errors")

        except Exception as e:
            logger.error(f"💥 Pipeline failed with exception: {e}")
            success = False

        return success

    def _run_hydrator(self) -> bool:
        """Run data hydration stage."""
        logger.info("📥 Stage 1: Data Hydration")

        try:
            # CourtListener
            if "courtlistener" in self.config.hydrator.get("sources", []):
                logger.info("  Fetching from CourtListener...")
                if not self._run_courtlistener_fetch():
                    return False

            # Wikipedia
            if "wikipedia" in self.config.hydrator.get("sources", []):
                logger.info("  Scraping from Wikipedia...")
                if not self._run_wikipedia_fetch():
                    return False

            # Index Constituents
            if "index_constituents" in self.config.hydrator.get("sources", []):
                logger.info("  Extracting index constituents...")
                if not self._run_index_constituents():
                    return False

            logger.info("✅ Hydration stage completed")
            return True

        except Exception as e:
            logger.error(f"❌ Hydration stage failed: {e}")
            return False

    def _run_cleaner(self) -> bool:
        """Run data cleaning stage."""
        logger.info("🧽 Stage 2: Data Cleaning")

        try:
            raw_docs = self.config.output_dir / "raw_documents.jsonl"
            if not raw_docs.exists():
                logger.error(f"Raw documents not found: {raw_docs}")
                return False

            normalized_docs = self.config.output_dir / "normalized_documents.jsonl"

            cmd = [
                sys.executable, "-m", "corpus_cleaner.cli.normalize",
                "--input", str(raw_docs),
                "--output", str(normalized_docs)
            ]

            # Add config if specified
            cleaner_config = self.config.cleaner.get("config_file")
            if cleaner_config:
                cmd.extend(["--config", str(cleaner_config)])

            logger.info(f"Running: {' '.join(cmd)}")

            if not self.config.dry_run:
                result = subprocess.run(cmd, cwd=Path.cwd())
                if result.returncode != 0:
                    logger.error(f"Cleaner failed with exit code {result.returncode}")
                    return False

            logger.info("✅ Cleaning stage completed")
            return True

        except Exception as e:
            logger.error(f"❌ Cleaning stage failed: {e}")
            return False

    def _run_extractors(self) -> bool:
        """Run information extraction stage."""
        logger.info("🔍 Stage 3: Information Extraction")

        try:
            normalized_docs = self.config.output_dir / "normalized_documents.jsonl"
            if not normalized_docs.exists():
                logger.error(f"Normalized documents not found: {normalized_docs}")
                return False

            # Run individual extractors
            extractors_config = self.config.extractors

            if extractors_config.get("quotes", {}).get("enabled", True):
                if not self._run_quote_extraction():
                    return False

            if extractors_config.get("outcomes", {}).get("enabled", True):
                if not self._run_outcome_extraction():
                    return False

            if extractors_config.get("cash_amounts", {}).get("enabled", True):
                if not self._run_cash_extraction():
                    return False

            # Combine results if requested
            if extractors_config.get("combine_results", True):
                if not self._combine_results():
                    return False

            logger.info("✅ Extraction stage completed")
            return True

        except Exception as e:
            logger.error(f"❌ Extraction stage failed: {e}")
            return False

    def _run_courtlistener_fetch(self) -> bool:
        """Run CourtListener data fetch."""
        try:
            # Create query config
            query_config = self.config.hydrator.get("courtlistener", {}).get("query", {})

            # Write temporary query file
            query_file = self.config.temp_dir / "courtlistener_query.yaml"
            with open(query_file, 'w') as f:
                yaml.safe_dump({"courtlistener": query_config}, f)

            output_dir = self.config.output_dir / "courtlistener"
            output_dir.mkdir(exist_ok=True)

            cmd = [
                sys.executable, "-m", "corpus_hydrator.cli.fetch",
                "courtlistener",
                "--query", str(query_file),
                "--output-dir", str(output_dir)
            ]

            # Add API key if available
            api_key = self.config.hydrator.get("courtlistener", {}).get("api_key")
            if api_key:
                cmd.extend(["--api-key", api_key])

            logger.info(f"Running CourtListener fetch: {' '.join(cmd)}")

            if not self.config.dry_run:
                result = subprocess.run(cmd, cwd=Path.cwd())
                if result.returncode != 0:
                    logger.error(f"CourtListener fetch failed with exit code {result.returncode}")
                    return False

            # Merge into main raw documents file
            self._merge_jsonl_files(
                output_dir / "documents.jsonl",
                self.config.output_dir / "raw_documents.jsonl"
            )

            return True

        except Exception as e:
            logger.error(f"CourtListener fetch failed: {e}")
            return False

    def _run_wikipedia_fetch(self) -> bool:
        """Run Wikipedia data fetch."""
        try:
            wiki_config = self.config.hydrator.get("wikipedia", {})

            for index in wiki_config.get("indices", ["sp500"]):
                cmd = [
                    sys.executable, "-m", "corpus_hydrator.cli.fetch",
                    "wikipedia",
                    "--index", index,
                    "--output-dir", str(self.config.output_dir)
                ]

                if wiki_config.get("dry_run"):
                    cmd.append("--dry-run")

                if wiki_config.get("max_companies"):
                    cmd.extend(["--max-companies", str(wiki_config["max_companies"])])

                if wiki_config.get("verbose"):
                    cmd.append("--verbose")

                logger.info(f"Running Wikipedia fetch for {index}: {' '.join(cmd)}")

                if not self.config.dry_run:
                    result = subprocess.run(cmd, cwd=Path.cwd())
                    if result.returncode != 0:
                        logger.error(f"Wikipedia fetch failed for {index} with exit code {result.returncode}")
                        return False

            return True

        except Exception as e:
            logger.error(f"Wikipedia fetch failed: {e}")
            return False

    def _run_index_constituents(self) -> bool:
        """Run index constituents extraction."""
        try:
            index_config = self.config.hydrator.get("index_constituents", {})

            for index in index_config.get("indices", ["sp500"]):
                cmd = [
                    sys.executable, "-m", "corpus_hydrator.cli.fetch",
                    "index-constituents",
                    "--index", index,
                    "--output-dir", str(self.config.output_dir)
                ]

                formats = index_config.get("formats", ["csv", "parquet"])
                for fmt in formats:
                    cmd.extend(["--format", fmt])

                if index_config.get("verbose"):
                    cmd.append("--verbose")

                logger.info(f"Running index constituents for {index}: {' '.join(cmd)}")

                if not self.config.dry_run:
                    result = subprocess.run(cmd, cwd=Path.cwd())
                    if result.returncode != 0:
                        logger.error(f"Index constituents failed for {index} with exit code {result.returncode}")
                        return False

            return True

        except Exception as e:
            logger.error(f"Index constituents failed: {e}")
            return False

    def _run_quote_extraction(self) -> bool:
        """Run quote extraction."""
        try:
            input_file = self.config.output_dir / "normalized_documents.jsonl"
            output_file = self.config.output_dir / "quotes.jsonl"

            cmd = [
                sys.executable, "-m", "corpus_extractors.cli.extract",
                "quotes",
                "--input", str(input_file),
                "--output", str(output_file)
            ]

            logger.info(f"Running quote extraction: {' '.join(cmd)}")

            if not self.config.dry_run:
                result = subprocess.run(cmd, cwd=Path.cwd())
                if result.returncode != 0:
                    logger.error(f"Quote extraction failed with exit code {result.returncode}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Quote extraction failed: {e}")
            return False

    def _run_outcome_extraction(self) -> bool:
        """Run outcome extraction."""
        try:
            input_file = self.config.output_dir / "normalized_documents.jsonl"
            output_file = self.config.output_dir / "outcomes.jsonl"

            cmd = [
                sys.executable, "-m", "corpus_extractors.cli.extract",
                "outcomes",
                "--input", str(input_file),
                "--output", str(output_file)
            ]

            logger.info(f"Running outcome extraction: {' '.join(cmd)}")

            if not self.config.dry_run:
                result = subprocess.run(cmd, cwd=Path.cwd())
                if result.returncode != 0:
                    logger.error(f"Outcome extraction failed with exit code {result.returncode}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Outcome extraction failed: {e}")
            return False

    def _run_cash_extraction(self) -> bool:
        """Run cash amount extraction."""
        try:
            input_file = self.config.output_dir / "normalized_documents.jsonl"
            output_file = self.config.output_dir / "cash_amounts.jsonl"

            cmd = [
                sys.executable, "-m", "corpus_extractors.cli.extract",
                "cash-amounts",
                "--input", str(input_file),
                "--output", str(output_file)
            ]

            logger.info(f"Running cash extraction: {' '.join(cmd)}")

            if not self.config.dry_run:
                result = subprocess.run(cmd, cwd=Path.cwd())
                if result.returncode != 0:
                    logger.error(f"Cash extraction failed with exit code {result.returncode}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Cash extraction failed: {e}")
            return False

    def _combine_results(self) -> bool:
        """Combine all extraction results."""
        try:
            input_file = self.config.output_dir / "normalized_documents.jsonl"
            combined_output = self.config.output_dir / "quotes_with_outcomes.jsonl"

            cmd = [
                sys.executable, "-m", "corpus_extractors.cli.extract",
                "combine",
                "--input", str(input_file),
                "--quotes-output", str(self.config.output_dir / "quotes_final.jsonl"),
                "--outcomes-output", str(self.config.output_dir / "outcomes_final.jsonl"),
                "--cash-amounts-output", str(self.config.output_dir / "cash_amounts_final.jsonl"),
                "--combined-output", str(combined_output)
            ]

            logger.info(f"Running result combination: {' '.join(cmd)}")

            if not self.config.dry_run:
                result = subprocess.run(cmd, cwd=Path.cwd())
                if result.returncode != 0:
                    logger.error(f"Result combination failed with exit code {result.returncode}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Result combination failed: {e}")
            return False

    def _merge_jsonl_files(self, source: Path, target: Path) -> None:
        """Merge JSONL files."""
        if not source.exists():
            return

        # Read source content
        source_lines = []
        with open(source, 'r') as f:
            source_lines = f.readlines()

        # Append to target
        with open(target, 'a') as f:
            f.writelines(source_lines)

    def _dry_run(self) -> bool:
        """Show what would be executed without running."""
        logger.info("🔍 DRY RUN - Showing execution plan:")

        # Simulate pipeline execution
        logger.info("📥 Stage 1: Data Hydration")
        logger.info("  Fetching from CourtListener...")
        logger.info("  Running CourtListener fetch...")

        logger.info("🧽 Stage 2: Data Cleaning")
        logger.info("  Running text normalization...")

        logger.info("🔍 Stage 3: Information Extraction")
        logger.info("  Running quote extraction...")
        logger.info("  Running outcome extraction...")
        logger.info("  Running cash extraction...")
        logger.info("  Running result combination...")

        logger.info("✅ Dry run completed - no files were modified")
        return True

    def _generate_summary(self) -> None:
        """Generate execution summary."""
        logger.info("📊 Pipeline Summary:")

        # Count files in output directory
        output_files = list(self.config.output_dir.glob("*"))
        logger.info(f"  Output directory: {self.config.output_dir}")
        logger.info(f"  Files generated: {len(output_files)}")

        for file_path in sorted(output_files):
            if file_path.is_file():
                size = file_path.stat().st_size
                logger.info(f"    {file_path.name}: {size:,} bytes")


@app.command()
def run(
    config_file: Optional[Path] = typer.Option(
        None,
        "--config",
        help="Pipeline configuration YAML file. If not provided, uses sensible defaults."
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output-dir",
        help="Output directory for all pipeline artifacts"
    ),
    stages: Optional[str] = typer.Option(
        None,
        "--stages",
        help="Comma-separated list of stages to run (hydrator,cleaner,extractors)"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be executed without actually running"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Continue execution even if a stage fails"
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Verbose output (equivalent to --log-level DEBUG)"
    )
):
    """
    Run the complete Corporate Speech Data Pipeline.

    This command orchestrates the entire data processing workflow:
    1. Data Ingestion (hydrator) - Fetch from CourtListener, Wikipedia, etc.
    2. Data Cleaning (cleaner) - Normalize and clean text documents
    3. Information Extraction (extractors) - Extract quotes, outcomes, cash amounts

    Examples:
        # Run with defaults
        uv run python scripts/pipeline_orchestrator.py run

        # Run with custom config
        uv run python scripts/pipeline_orchestrator.py run --config my_pipeline.yaml

        # Run only specific stages
        uv run python scripts/pipeline_orchestrator.py run --stages hydrator,cleaner

        # Dry run to see execution plan
        uv run python scripts/pipeline_orchestrator.py run --dry-run
    """

    # Load configuration
    if config_file and config_file.exists():
        logger.info(f"Loading configuration from {config_file}")
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        config = PipelineConfig.from_dict(config_data)
    else:
        logger.info("Using default configuration")
        config = PipelineConfig.get_defaults()

    # Override with command line options
    if output_dir:
        config.output_dir = output_dir
    if stages:
        config.stages = set(s.strip() for s in stages.split(","))
    if dry_run:
        config.dry_run = dry_run
    if force:
        config.force = force
    if verbose:
        config.log_level = "DEBUG"
    else:
        config.log_level = log_level

    # Create orchestrator and run
    orchestrator = PipelineOrchestrator(config)
    success = orchestrator.run_pipeline()

    if success:
        logger.info("🎉 Pipeline execution completed successfully!")
        sys.exit(0)
    else:
        logger.error("💥 Pipeline execution failed!")
        sys.exit(1)


@app.command()
def generate_config(
    output_file: Path = typer.Option(
        "pipeline_config.yaml",
        "--output",
        help="Output configuration file path"
    ),
    include_comments: bool = typer.Option(
        True,
        "--comments/--no-comments",
        help="Include explanatory comments in the config"
    )
):
    """
    Generate a sample pipeline configuration file.

    This creates a comprehensive configuration file with all available options
    and sensible defaults, which can be customized for specific use cases.
    """

    config = PipelineConfig.get_defaults()
    config_dict = config.to_dict()

    if include_comments:
        # Add comprehensive comments
        config_dict = add_config_comments(config_dict)

    with open(output_file, 'w') as f:
        yaml.safe_dump(config_dict, f, default_flow_style=False, sort_keys=False)

    logger.info(f"✅ Generated configuration file: {output_file}")
    logger.info("Edit this file to customize your pipeline configuration.")


def add_config_comments(config: Dict[str, Any]) -> Dict[str, Any]:
    """Add explanatory comments to configuration dictionary."""

    # This is a simplified version - in practice you'd want more comprehensive comments
    commented_config = {}

    # Add header comment
    commented_config["_comment"] = "Corporate Speech Data Pipeline Configuration"
    commented_config["# Global Settings"] = None
    commented_config.update(config)

    return config  # For now, just return the original


@app.command()
def validate_config(
    config_file: Path = typer.Argument(..., help="Configuration file to validate")
):
    """
    Validate a pipeline configuration file.

    Checks that the configuration file is properly formatted and contains
    valid options.
    """

    try:
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)

        # Try to create config object
        config = PipelineConfig.from_dict(config_data)

        logger.info("✅ Configuration file is valid")
        logger.info(f"Output directory: {config.output_dir}")
        logger.info(f"Stages to run: {', '.join(sorted(config.stages))}")

    except Exception as e:
        logger.error(f"❌ Configuration validation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    app()
