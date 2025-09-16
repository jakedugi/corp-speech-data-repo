"""
File I/O utilities for CourtListener operations.

This module contains utilities for downloading files, checking RECAP status,
and managing file operations.
"""

import json
import os
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urljoin

import httpx
from loguru import logger


def ensure_dir(path: Path) -> None:
    """Ensure directory exists, creating it if necessary.

    Args:
        path: Directory path to ensure exists
    """
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    """Load JSON data from file.

    Args:
        path: Path to JSON file

    Returns:
        Parsed JSON data or None if file doesn't exist
    """
    if path.exists():
        with open(path) as fh:
            return json.load(fh)
    return None


def download(url: str, dest: Path, timeout: int = 30) -> None:
    """Download file from URL to destination.

    Args:
        url: URL to download from
        dest: Destination path
        timeout: Request timeout in seconds
    """
    logger.info(f"Downloading {url} to {dest}")
    with httpx.Client(timeout=timeout) as client:
        resp = client.get(url)
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(resp.content)
    logger.info(f"Downloaded {url} to {dest}")


def needs_recap_fetch(ia_dump_path: Path) -> bool:
    """Check if docket needs RECAP fetch based on IA dump analysis.

    Args:
        ia_dump_path: Path to IA dump JSON file

    Returns:
        True if RECAP fetch is needed
    """
    if not ia_dump_path.exists():
        return True

    try:
        with open(ia_dump_path) as f:
            ia_data = json.load(f)

        # Check if there are gaps in document coverage
        # This is a simplified check - in practice you'd analyze the IA data structure
        if "docket_entries" in ia_data:
            for entry in ia_data["docket_entries"]:
                if entry.get("recap_documents"):
                    for doc in entry["recap_documents"]:
                        # Check if document is available but PDF is missing
                        if doc.get("is_available") and not doc.get("filepath_local"):
                            return True

        return False
    except Exception as e:
        logger.warning(f"Error checking RECAP fetch need: {e}")
        return True


def download_missing_pdfs(
    base_url: str,
    documents: list,
    output_dir: Path,
    timeout: int = 30
) -> None:
    """Download missing PDFs for RECAP documents.

    Args:
        base_url: Base URL for CourtListener
        documents: List of document metadata
        output_dir: Output directory for PDFs
        timeout: Request timeout in seconds
    """
    ensure_dir(output_dir)

    for doc in documents:
        if not doc.get("filepath_local"):
            continue

        if doc.get("is_available") is False:
            logger.warning(f"PDF {doc['id']} is marked unavailable—skipping.")
            continue

        pdf_url = urljoin(base_url, doc["filepath_local"])
        pdf_dest = output_dir / f"{doc['id']}.pdf"

        if pdf_dest.exists():
            continue

        if not pdf_url.startswith("http"):
            logger.warning(f"Skipping invalid URL: {pdf_url}")
            continue

        try:
            download(pdf_url, pdf_dest, timeout)
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code in (403, 429):
                logger.warning(f"PDF {doc['id']} returned HTTP {code}—skipping.")
            else:
                raise
        except Exception as e:
            logger.warning(f"Failed to download PDF for doc {doc['id']}: {e}")
