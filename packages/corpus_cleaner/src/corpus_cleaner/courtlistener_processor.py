"""
CourtListener data processor for the cleaner module.

This module handles navigation of CourtListener directory structures created by the hydrator,
extracts plain text from entries and opinions, cleans it, and outputs normalized JSONL
documents ready for the extractor module.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from .cleaner import TextCleaner

logger = logging.getLogger(__name__)


class CourtListenerProcessor:
    """
    Processes CourtListener data from hydrator output directories.

    Navigates case directories, extracts text from entries and opinions,
    applies cleaning/normalization, and outputs JSONL documents.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the processor.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.text_cleaner = TextCleaner(config)

    def discover_cases(self, base_dir: Path) -> List[Path]:
        """
        Discover all case directories in the CourtListener data directory.

        Args:
            base_dir: Base CourtListener directory (e.g., CourtListener/)

        Returns:
            List of case directory paths
        """
        base_dir = Path(base_dir)

        if not base_dir.exists():
            raise ValueError(f"Directory does not exist: {base_dir}")

        # Find all case directories (they have structure like "1:22-cv-10979_nysd")
        case_dirs = []
        for item in base_dir.iterdir():
            if item.is_dir() and not item.name.startswith("."):
                # Check if it looks like a case directory (has entries or opinions subdirs)
                if (item / "entries").exists() or (item / "opinions").exists():
                    case_dirs.append(item)

        logger.info(f"Discovered {len(case_dirs)} case directories in {base_dir}")
        return sorted(case_dirs)

    def extract_entries(self, case_dir: Path) -> Iterator[Dict[str, Any]]:
        """
        Extract documents from the entries directory of a case.

        Args:
            case_dir: Path to case directory

        Yields:
            Document dictionaries with metadata and plain_text
        """
        entries_dir = case_dir / "entries"

        if not entries_dir.exists():
            logger.debug(f"No entries directory in {case_dir.name}")
            return

        # Find all JSON files that are document files (not metadata)
        doc_files = sorted(entries_dir.glob("doc_*.json"))

        for doc_file in doc_files:
            try:
                with open(doc_file, "r", encoding="utf-8") as f:
                    doc_data = json.load(f)

                # Only process if there's actual text content
                plain_text = doc_data.get("plain_text", "")
                if plain_text and plain_text.strip():
                    yield {
                        "doc_data": doc_data,
                        "file_path": doc_file,
                        "source_type": "entry",
                        "case_dir": case_dir,
                    }
                else:
                    logger.debug(f"Skipping {doc_file.name} (no plain_text)")

            except Exception as e:
                logger.error(f"Error reading {doc_file}: {e}")
                continue

    def extract_opinions(self, case_dir: Path) -> Iterator[Dict[str, Any]]:
        """
        Extract documents from the opinions directory of a case.

        Args:
            case_dir: Path to case directory

        Yields:
            Document dictionaries with metadata and plain_text
        """
        opinions_dir = case_dir / "opinions"

        if not opinions_dir.exists():
            logger.debug(f"No opinions directory in {case_dir.name}")
            return

        # Find all JSON files with opinions
        opinion_files = sorted(opinions_dir.glob("opinions_*.json"))

        for opinion_file in opinion_files:
            try:
                with open(opinion_file, "r", encoding="utf-8") as f:
                    opinion_data = json.load(f)

                # Only process if there's actual text content
                plain_text = opinion_data.get("plain_text", "")
                if plain_text and plain_text.strip():
                    yield {
                        "doc_data": opinion_data,
                        "file_path": opinion_file,
                        "source_type": "opinion",
                        "case_dir": case_dir,
                    }
                else:
                    logger.debug(f"Skipping {opinion_file.name} (no plain_text)")

            except Exception as e:
                logger.error(f"Error reading {opinion_file}: {e}")
                continue

    def extract_case_documents(
        self,
        case_dir: Path,
        include_entries: bool = True,
        include_opinions: bool = True,
    ) -> Iterator[Dict[str, Any]]:
        """
        Extract all documents from a case directory.

        Args:
            case_dir: Path to case directory
            include_entries: Whether to include entries
            include_opinions: Whether to include opinions

        Yields:
            Raw document dictionaries
        """
        # Process entries first (primary)
        if include_entries:
            yield from self.extract_entries(case_dir)

        # Then process opinions (secondary)
        if include_opinions:
            yield from self.extract_opinions(case_dir)

    def create_normalized_document(
        self, raw_doc: Dict[str, Any], case_id: str
    ) -> Dict[str, Any]:
        """
        Create a normalized document from raw CourtListener data.

        Args:
            raw_doc: Raw document dictionary from extract methods
            case_id: Case identifier

        Returns:
            Normalized document dictionary
        """
        doc_data = raw_doc["doc_data"]
        source_type = raw_doc["source_type"]
        file_path = raw_doc["file_path"]

        # Extract plain text and clean it
        original_text = doc_data.get("plain_text", "")
        cleaned_text = self.text_cleaner.clean(original_text)

        # Create document ID
        doc_id = self._generate_doc_id(doc_data, source_type, case_id)

        # Build metadata
        meta = self._extract_metadata(doc_data, source_type, case_id)

        # Create normalized document in the format expected by extractors
        normalized_doc = {
            "schema_version": "1.0",
            "doc_id": doc_id,
            "source_uri": doc_data.get("resource_uri", ""),
            "retrieved_at": doc_data.get("date_created", datetime.now().isoformat()),
            "raw_text": cleaned_text,
            "meta": meta,
            "_source": {
                "type": source_type,
                "file_path": str(file_path),
                "case_id": case_id,
                "original_id": doc_data.get("id"),
                "original_length": len(original_text),
                "normalized_length": len(cleaned_text),
            },
        }

        return normalized_doc

    def _generate_doc_id(
        self, doc_data: Dict[str, Any], source_type: str, case_id: str
    ) -> str:
        """Generate a unique document ID."""
        doc_id = doc_data.get("id", "unknown")
        return f"{case_id}_{source_type}_{doc_id}"

    def _extract_metadata(
        self, doc_data: Dict[str, Any], source_type: str, case_id: str
    ) -> Dict[str, Any]:
        """
        Extract relevant metadata from CourtListener document.

        Args:
            doc_data: Raw document data from CourtListener
            source_type: Type of document (entry or opinion)
            case_id: Case identifier

        Returns:
            Metadata dictionary
        """
        meta = {
            "case_id": case_id,
            "source_type": source_type,
        }

        # Add common fields
        if "date_created" in doc_data:
            meta["date_created"] = doc_data["date_created"]
        if "date_modified" in doc_data:
            meta["date_modified"] = doc_data["date_modified"]

        # Add type-specific fields
        if source_type == "opinion":
            # Opinion-specific metadata
            if "cluster_id" in doc_data:
                meta["cluster_id"] = doc_data["cluster_id"]
            if "type" in doc_data:
                meta["opinion_type"] = doc_data["type"]
            if "author_str" in doc_data:
                meta["author"] = doc_data["author_str"]
            if "page_count" in doc_data:
                meta["page_count"] = doc_data["page_count"]
            if "sha1" in doc_data:
                meta["sha1"] = doc_data["sha1"]
            if "absolute_url" in doc_data:
                meta["absolute_url"] = doc_data["absolute_url"]

        elif source_type == "entry":
            # Entry-specific metadata
            if "document_number" in doc_data:
                meta["document_number"] = doc_data["document_number"]
            if "description" in doc_data:
                meta["description"] = doc_data["description"]
            if "document_type" in doc_data:
                meta["document_type"] = doc_data["document_type"]
            if "pacer_doc_id" in doc_data:
                meta["pacer_doc_id"] = doc_data["pacer_doc_id"]
            if "absolute_url" in doc_data:
                meta["absolute_url"] = doc_data["absolute_url"]

        return meta

    def process_case(
        self,
        case_dir: Path,
        include_entries: bool = True,
        include_opinions: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Process a single case directory and return normalized documents.

        Args:
            case_dir: Path to case directory
            include_entries: Whether to include entries
            include_opinions: Whether to include opinions

        Returns:
            List of normalized documents
        """
        case_id = case_dir.name
        logger.info(f"Processing case: {case_id}")

        normalized_docs = []

        # Extract and normalize all documents
        for raw_doc in self.extract_case_documents(
            case_dir, include_entries=include_entries, include_opinions=include_opinions
        ):
            try:
                normalized_doc = self.create_normalized_document(raw_doc, case_id)
                normalized_docs.append(normalized_doc)
            except Exception as e:
                logger.error(
                    f"Error normalizing document from {raw_doc['file_path']}: {e}"
                )
                continue

        logger.info(f"Processed {len(normalized_docs)} documents from case {case_id}")
        return normalized_docs

    def process_all_cases(
        self,
        base_dir: Path,
        output_file: Path,
        include_entries: bool = True,
        include_opinions: bool = True,
        case_filter: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """
        Process all cases in the CourtListener directory and write to output.

        Args:
            base_dir: Base CourtListener directory
            output_file: Output JSONL file path
            include_entries: Whether to include entries
            include_opinions: Whether to include opinions
            case_filter: Optional list of case IDs to process (if None, process all)

        Returns:
            Statistics dictionary
        """
        logger.info(f"Processing CourtListener data from: {base_dir}")
        logger.info(f"Output file: {output_file}")

        # Discover cases
        case_dirs = self.discover_cases(base_dir)

        # Filter cases if specified
        if case_filter:
            case_dirs = [d for d in case_dirs if d.name in case_filter]
            logger.info(f"Filtered to {len(case_dirs)} cases")

        # Statistics
        stats = {
            "total_cases": len(case_dirs),
            "total_documents": 0,
            "entries_count": 0,
            "opinions_count": 0,
            "errors": 0,
        }

        # Process all cases and write to output
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as outfile:
            for case_dir in case_dirs:
                try:
                    normalized_docs = self.process_case(
                        case_dir,
                        include_entries=include_entries,
                        include_opinions=include_opinions,
                    )

                    # Write documents
                    for doc in normalized_docs:
                        outfile.write(json.dumps(doc, ensure_ascii=False) + "\n")

                        # Update stats
                        stats["total_documents"] += 1
                        if doc["_source"]["type"] == "entry":
                            stats["entries_count"] += 1
                        elif doc["_source"]["type"] == "opinion":
                            stats["opinions_count"] += 1

                except Exception as e:
                    logger.error(f"Error processing case {case_dir.name}: {e}")
                    stats["errors"] += 1
                    continue

        # Log summary
        logger.info(f"Processing complete!")
        logger.info(f"  Cases processed: {stats['total_cases']}")
        logger.info(f"  Total documents: {stats['total_documents']}")
        logger.info(f"  Entries: {stats['entries_count']}")
        logger.info(f"  Opinions: {stats['opinions_count']}")
        logger.info(f"  Errors: {stats['errors']}")
        logger.info(f"Output written to: {output_file}")

        return stats
