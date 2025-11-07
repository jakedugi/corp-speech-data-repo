"""
Main QuoteExtractor implementation that orchestrates the full extraction pipeline.
This consolidates functionality from multiple scattered extractor classes.
"""

import json
from pathlib import Path

# Optional yaml import
try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False
    yaml = None
from typing import Any, Dict, Iterator, List, Optional, Set

from corpus_cleaner.cleaner import TextCleaner
from corpus_types.schemas.models import QuoteCandidate
from loguru import logger

from .attribution import Attributor
from .first_pass import FirstPassExtractor
from .rerank import SemanticReranker


class QuoteExtractor:
    """
    Main quote extractor that orchestrates the full pipeline:
    1. First pass extraction (regex + keyword filtering)
    2. Attribution (speaker identification)
    3. Semantic reranking
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the quote extractor with configuration.

        Args:
            config: Configuration dictionary. If None, loads default from configs/quotes.yaml.
                   User config is merged over defaults.
        """
        # Load default configuration
        default_config = self._load_default_config()

        # Merge user config over defaults (deep merge)
        self.config = self._merge_configs(default_config, config or {})

        # Extract configuration values
        keywords = self.config["extraction"]["keywords"]
        company_aliases = set(self.config["extraction"]["company_aliases"])
        seed_quotes = self.config["reranking"]["seed_quotes"]
        threshold = self.config["reranking"]["threshold"]
        spacy_model = self.config["nlp"]["spacy_model"]
        role_keywords = self.config["nlp"]["role_keywords"]

        # Extract new configuration options
        executive_names = self.config["nlp"].get("executive_names", [])
        enable_ner = self.config["nlp"].get("enable_ner", True)
        use_gpu = self.config["nlp"].get("use_gpu", False)

        # Initialize pipeline components
        self.cleaner = TextCleaner()
        self.first_pass = FirstPassExtractor(keywords, self.cleaner)
        self.attributor = Attributor(
            company_aliases=company_aliases,
            spacy_model=spacy_model,
            role_keywords=role_keywords,
            executive_names=executive_names,
            enable_ner=enable_ner,
            use_gpu=use_gpu,
        )
        self.reranker = SemanticReranker(seed_quotes, threshold)

        logger.info(
            "QuoteExtractor initialized with comprehensive S&P 500 corporate data"
        )

    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration from quotes_comprehensive.py or fallback to quotes.yaml."""
        # Try comprehensive config first
        comprehensive_config_path = (
            Path(__file__).parent.parent.parent / "configs" / "quotes_comprehensive.py"
        )
        if comprehensive_config_path.exists():
            try:
                # Import the config from Python file
                import sys

                config_dir = str(comprehensive_config_path.parent)
                if config_dir not in sys.path:
                    sys.path.insert(0, config_dir)
                from quotes_comprehensive import config as comprehensive_config

                logger.info(
                    f"Loaded comprehensive config from {comprehensive_config_path}"
                )
                return comprehensive_config
            except Exception as e:
                logger.warning(f"Failed to load comprehensive config: {e}")

        # Fallback to YAML config if available
        if HAS_YAML:
            yaml_config_path = (
                Path(__file__).parent.parent.parent / "configs" / "quotes.yaml"
            )
            try:
                with open(yaml_config_path, "r") as f:
                    config = yaml.safe_load(f)
                    logger.info(f"Loaded YAML config from {yaml_config_path}")
                    return config
            except FileNotFoundError:
                pass

        logger.warning(
            f"No config files found or YAML not available, using minimal defaults"
        )
        return self._get_minimal_defaults()

    def _get_minimal_defaults(self) -> Dict[str, Any]:
        """Provide minimal hardcoded defaults if config file is missing."""
        return {
            "nlp": {
                "spacy_model": "en_core_web_sm",
                "role_keywords": ["CEO", "CFO", "President", "Officer", "Director"],
            },
            "extraction": {
                "keywords": [
                    "regulation",
                    "policy",
                    "statement",
                    "violation",
                    "compliance",
                ],
                "company_aliases": ["company", "corporation", "inc", "llc"],
            },
            "reranking": {
                "seed_quotes": ["The company stated that", "According to the policy"],
                "threshold": 0.55,
                "model": "all-mpnet-base-v2",
            },
        }

    def _merge_configs(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deep merge override config into base config."""
        result = base.copy()
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        return result

    def extract_quotes(self, doc_text: str) -> List[QuoteCandidate]:
        """
        Extract quotes method for test compatibility.
        Returns a list instead of iterator.
        """
        return list(self.extract(doc_text))

    def extract(self, doc_text: str) -> Iterator[QuoteCandidate]:
        """
        Extract and process quotes through the full pipeline.

        Args:
            doc_text: The full text of the document

        Yields:
            Fully processed QuoteCandidate objects
        """
        # Clean the document text
        cleaned_text = self.cleaner.clean(doc_text)

        # First pass: extract potential quotes
        candidates = list(self.first_pass.extract(cleaned_text))
        if not candidates:
            return

        logger.debug(f"First pass found {len(candidates)} candidates")

        # Attribution: identify speakers
        attributed = list(self.attributor.filter(candidates))
        if not attributed:
            return

        logger.debug(f"Attribution found {len(attributed)} attributed quotes")

        # Semantic reranking: filter by similarity to seed quotes
        final_quotes = list(self.reranker.rerank(attributed))
        logger.debug(f"Reranking found {len(final_quotes)} final quotes")

        yield from final_quotes

    def process_file(self, input_file: Path, output_file: Path) -> None:
        """
        Process a single file and save extracted quotes to output file.

        Args:
            input_file: Path to the input file to process
            output_file: Path to save the results
        """
        try:
            # For compatibility with test, assume input is JSON with text field
            import json

            if input_file.suffix == ".json":
                data = json.loads(input_file.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    all_quotes = []
                    for item in data:
                        text = item.get("opinion_text", "")
                        quotes = list(self.extract(text))
                        all_quotes.extend([q.to_dict() for q in quotes])
                    output_file.write_text(json.dumps(all_quotes, indent=2))
                else:
                    text = data.get("opinion_text", "")
                    quotes = list(self.extract(text))
                    output_file.write_text(
                        json.dumps([q.to_dict() for q in quotes], indent=2)
                    )
            else:
                # For text files, process directly
                text = input_file.read_text(encoding="utf-8")
                quotes = list(self.extract(text))
                import json

                output_file.write_text(
                    json.dumps([q.to_dict() for q in quotes], indent=2)
                )
        except Exception as e:
            logger.error(f"Error processing file {input_file}: {e}")

    def process_directory(self, input_dir: Path, output_dir: Path) -> None:
        """
        Process all files in input directory and save results to output directory.

        Args:
            input_dir: Directory containing files to process
            output_dir: Directory to save results
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        for input_file in input_dir.glob("*.json"):
            if input_file.is_file():
                logger.info(f"Processing {input_file}")
                output_file = output_dir / f"processed_{input_file.name}"
                self.process_file(input_file, output_file)
