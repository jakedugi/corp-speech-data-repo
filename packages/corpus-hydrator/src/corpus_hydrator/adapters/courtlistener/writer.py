"""
CourtListener Writer

This module contains writers for formatting and outputting CourtListener data.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from loguru import logger

from .utils.file_io import ensure_dir


class CourtListenerWriter:
    """Writer for CourtListener data output."""

    def __init__(self, output_dir: Path):
        """Initialize the writer.

        Args:
            output_dir: Base output directory
        """
        self.output_dir = Path(output_dir)
        self.logger = logger.bind(writer="courtlistener")

    def write_opinions(
        self,
        opinions: List[Dict[str, Any]],
        statute_name: str,
        chunk_idx: Optional[int] = None
    ) -> Path:
        """Write opinion data to files.

        Args:
            opinions: List of opinion data
            statute_name: Name of the statute
            chunk_idx: Chunk index for batched queries

        Returns:
            Path to output directory
        """
        from .core.processor import slugify

        base_name = slugify(statute_name)
        if chunk_idx is not None:
            output_dir = self.output_dir / "raw" / "courtlistener" / base_name / f"chunk_{chunk_idx}"
        else:
            output_dir = self.output_dir / "raw" / "courtlistener" / base_name

        ensure_dir(output_dir)

        for opinion in opinions:
            # Write metadata
            metadata_path = output_dir / f"opinion_{opinion['id']}_metadata.json"
            with open(metadata_path, "w") as f:
                json.dump(opinion, f, indent=2)

            # Write plain text if available
            if opinion.get("plain_text"):
                text_path = output_dir / f"opinion_{opinion['id']}_text.txt"
                with open(text_path, "w") as f:
                    f.write(opinion["plain_text"])

        self.logger.info(f"Wrote {len(opinions)} opinions to {output_dir}")
        return output_dir

    def write_dockets(self, dockets: List[Dict[str, Any]], output_dir: Optional[Path] = None) -> Path:
        """Write docket data to files.

        Args:
            dockets: List of docket data
            output_dir: Optional output directory override

        Returns:
            Path to output directory
        """
        if output_dir is None:
            output_dir = self.output_dir / "dockets"
        ensure_dir(output_dir)

        for docket in dockets:
            docket_path = output_dir / f"docket_{docket['id']}.json"
            with open(docket_path, "w") as f:
                json.dump(docket, f, indent=2)

        self.logger.info(f"Wrote {len(dockets)} dockets to {output_dir}")
        return output_dir

    def write_docket_entries(
        self,
        entries: List[Dict[str, Any]],
        docket_id: int,
        output_dir: Optional[Path] = None
    ) -> Path:
        """Write docket entries to files.

        Args:
            entries: List of docket entry data
            docket_id: Docket ID
            output_dir: Optional output directory override

        Returns:
            Path to output directory
        """
        if output_dir is None:
            output_dir = self.output_dir / "docket_entries" / f"docket_{docket_id}"
        ensure_dir(output_dir)

        for entry in entries:
            entry_path = output_dir / f"entry_{entry.get('id', 'unknown')}_metadata.json"
            with open(entry_path, "w") as f:
                json.dump(entry, f, indent=2)

            # Handle nested RECAP documents
            if entry.get("recap_documents"):
                docs_dir = output_dir / f"entry_{entry.get('id', 'unknown')}_documents"
                ensure_dir(docs_dir)

                for doc in entry["recap_documents"]:
                    doc_meta_path = docs_dir / f"doc_{doc.get('id', 'unknown')}_metadata.json"
                    with open(doc_meta_path, "w") as f:
                        json.dump(doc, f, indent=2)

                    if doc.get("plain_text"):
                        doc_text_path = docs_dir / f"doc_{doc.get('id', 'unknown')}_text.txt"
                        with open(doc_text_path, "w") as f:
                            f.write(doc["plain_text"])

        self.logger.info(f"Wrote {len(entries)} docket entries to {output_dir}")
        return output_dir

    def write_recap_documents(
        self,
        documents: List[Dict[str, Any]],
        docket_id: Optional[int] = None,
        entry_id: Optional[int] = None,
        output_dir: Optional[Path] = None
    ) -> Path:
        """Write RECAP documents to files.

        Args:
            documents: List of document data
            docket_id: Optional docket ID
            entry_id: Optional entry ID
            output_dir: Optional output directory override

        Returns:
            Path to output directory
        """
        if output_dir is None:
            if docket_id:
                output_dir = self.output_dir / "recap_documents" / f"docket_{docket_id}"
            elif entry_id:
                output_dir = self.output_dir / "recap_documents" / f"entry_{entry_id}"
            else:
                output_dir = self.output_dir / "recap_documents" / "search"

        ensure_dir(output_dir)

        for doc in documents:
            # Write metadata
            doc_meta_path = output_dir / f"doc_{doc.get('id', 'unknown')}_metadata.json"
            with open(doc_meta_path, "w") as f:
                json.dump(doc, f, indent=2)

            # Write plain text if available
            if doc.get("plain_text"):
                doc_text_path = output_dir / f"doc_{doc.get('id', 'unknown')}_text.txt"
                with open(doc_text_path, "w") as f:
                    f.write(doc["plain_text"])

        self.logger.info(f"Wrote {len(documents)} RECAP documents to {output_dir}")
        return output_dir

    def write_full_docket(
        self,
        docket_meta: Dict[str, Any],
        entries: List[Dict[str, Any]],
        docket_id: int,
        output_dir: Optional[Path] = None
    ) -> Path:
        """Write complete docket data to files.

        Args:
            docket_meta: Docket metadata
            entries: List of docket entries
            docket_id: Docket ID
            output_dir: Optional output directory override

        Returns:
            Path to output directory
        """
        if output_dir is None:
            output_dir = self.output_dir / "full_dockets" / f"docket_{docket_id}"

        ensure_dir(output_dir)

        # Write docket metadata
        docket_path = output_dir / "docket_info.json"
        with open(docket_path, "w") as f:
            json.dump(docket_meta, f, indent=2)

        # Write entries
        entries_dir = output_dir / "entries"
        ensure_dir(entries_dir)
        for entry in entries:
            entry_path = entries_dir / f"entry_{entry.get('id')}_metadata.json"
            with open(entry_path, "w") as f:
                json.dump(entry, f, indent=2)

        # Write summary
        summary = {
            "docket_id": docket_id,
            "entry_count": len(entries),
            "output_dir": str(output_dir),
        }
        summary_path = output_dir / "summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        self.logger.info(f"Wrote complete docket {docket_id} to {output_dir}")
        return output_dir
