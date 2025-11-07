"""
Data Writer with Manifest Generation

This module handles writing index constituent data to files with
self-describing manifests, deterministic output, and support for
multiple formats (CSV, Parquet).
"""

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from corpus_types.schemas.models import IndexConstituent

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0"


def to_dataframe(constituents: List[IndexConstituent]) -> pd.DataFrame:
    """
    Convert list of IndexConstituent objects to pandas DataFrame.

    Args:
        constituents: List of IndexConstituent objects

    Returns:
        DataFrame with constituent data
    """
    data = []
    for constituent in constituents:
        data.append(
            {
                "symbol": constituent.symbol,
                "company_name": constituent.company_name,
                "index_name": constituent.index_name,
                "sector": constituent.sector,
                "industry": constituent.industry,
                "date_added": constituent.date_added,
                "extracted_at": constituent.extracted_at.isoformat(),
                "source_url": constituent.source_url,
            }
        )

    df = pd.DataFrame(data)
    return df


def write_csv(df: pd.DataFrame, output_path: Path) -> str:
    """
    Write DataFrame to CSV file.

    Args:
        df: DataFrame to write
        output_path: Output file path

    Returns:
        SHA256 hash of the written file
    """
    df.to_csv(output_path, index=False)
    logger.info(f"Wrote {len(df)} rows to {output_path}")

    # Calculate hash
    with open(output_path, "rb") as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()

    return file_hash


def write_parquet(df: pd.DataFrame, output_path: Path) -> str:
    """
    Write DataFrame to Parquet file.

    Args:
        df: DataFrame to write
        output_path: Output file path

    Returns:
        SHA256 hash of the written file
    """
    df.to_parquet(output_path, index=False)
    logger.info(f"Wrote {len(df)} rows to {output_path}")

    # Calculate hash
    with open(output_path, "rb") as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()

    return file_hash


def generate_manifest(
    index_name: str,
    row_count: int,
    extracted_at: str,
    source_url: Optional[str],
    csv_hash: str,
    parquet_hash: str,
) -> Dict[str, Any]:
    """
    Generate manifest dictionary for the dataset.

    Args:
        index_name: Name of the index
        row_count: Number of rows in the dataset
        extracted_at: ISO timestamp of extraction
        source_url: Source URL of the data
        csv_hash: SHA256 hash of CSV file
        parquet_hash: SHA256 hash of Parquet file

    Returns:
        Manifest dictionary
    """
    return {
        "index_name": index_name,
        "rows": row_count,
        "schema_version": SCHEMA_VERSION,
        "extracted_at": extracted_at,
        "source_url": source_url,
        "sha256_csv": csv_hash,
        "sha256_parquet": parquet_hash,
        "format": "index_constituents",
        "description": f"Constituents for {index_name}",
    }


def write_bundle(
    constituents: List[IndexConstituent], output_dir: Path, formats: List[str] = None
) -> Dict[str, Any]:
    """
    Write complete data bundle with files and manifest.

    Args:
        constituents: List of IndexConstituent objects
        output_dir: Output directory
        formats: List of formats to write (default: ['csv', 'parquet'])

    Returns:
        Manifest dictionary
    """
    if formats is None:
        formats = ["csv", "parquet"]

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert to DataFrame (ensures deterministic order)
    df = to_dataframe(constituents)

    if len(df) == 0:
        logger.warning("No constituents to write")
        return {}

    # Get index name from first constituent
    index_name = constituents[0].index_name
    index_key = index_name.lower().replace(" ", "").replace("&", "")

    # Write files
    csv_hash = None
    parquet_hash = None

    if "csv" in formats:
        csv_path = output_dir / f"{index_key}_constituents.csv"
        csv_hash = write_csv(df, csv_path)

    if "parquet" in formats:
        parquet_path = output_dir / f"{index_key}_constituents.parquet"
        parquet_hash = write_parquet(df, parquet_path)

    # Generate manifest
    manifest = generate_manifest(
        index_name=index_name,
        row_count=len(df),
        extracted_at=constituents[0].extracted_at.isoformat(),
        source_url=constituents[0].source_url,
        csv_hash=csv_hash,
        parquet_hash=parquet_hash,
    )

    # Write manifest
    manifest_path = output_dir / f"{index_key}_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Wrote bundle for {index_name} to {output_dir}")
    return manifest
