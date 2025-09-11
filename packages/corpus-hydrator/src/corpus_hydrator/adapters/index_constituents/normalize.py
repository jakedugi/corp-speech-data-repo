"""
Data Normalization for Index Constituents

This module handles column mapping and data transformation from raw parsed data
to standardized IndexConstituent objects with proper validation.
"""

import logging
from typing import Dict, Any, Optional

from corpus_types.schemas.models import IndexConstituent

logger = logging.getLogger(__name__)


def normalize_row(row: Dict[str, Any], index_key: str, index_name: str) -> Optional[IndexConstituent]:
    """
    Normalize a raw row dictionary into an IndexConstituent object.

    Args:
        row: Raw row data from parser
        index_key: Index identifier (e.g., 'sp500', 'dow')
        index_name: Full index name (e.g., 'S&P 500', 'Dow Jones Industrial Average')

    Returns:
        IndexConstituent object or None if normalization fails
    """
    try:
        # Map column names based on index type
        if index_key == 'dow':
            # Dow Jones has different column structure
            symbol = row.get('Symbol')
            company_name = row.get('Company')
            industry = row.get('Industry')
            date_added = row.get('Date added')
            sector = None  # Dow Jones doesn't have sector info
        else:
            # Standard mapping for S&P 500 and Nasdaq 100
            symbol = row.get('Symbol') or row.get('Ticker')
            company_name = row.get('Security') or row.get('Company')
            sector = row.get('GICS Sector')
            industry = row.get('Industry') or row.get('GICS Sub-Industry')
            date_added = row.get('Date first added') or row.get('Date added')

        # Validate required fields
        if not symbol or not company_name:
            logger.debug(f"Skipping row missing symbol or company_name: {row}")
            return None

        # Create IndexConstituent with validation
        constituent = IndexConstituent(
            symbol=symbol,
            company_name=company_name,
            index_name=index_name,
            sector=sector,
            industry=industry,
            date_added=date_added,
            source_url=f"https://en.wikipedia.org/wiki/{_get_wikipedia_page(index_key)}"
        )

        logger.debug(f"Normalized constituent: {constituent.symbol}")
        return constituent

    except Exception as e:
        logger.warning(f"Failed to normalize row {row}: {e}")
        return None


def _get_wikipedia_page(index_key: str) -> str:
    """Get Wikipedia page name for index."""
    page_map = {
        'sp500': 'List_of_S%26P_500_companies',
        'dow': 'Dow_Jones_Industrial_Average',
        'nasdaq100': 'NASDAQ-100'
    }
    return page_map.get(index_key, index_key)


def normalize_rows(rows: list, index_key: str, index_name: str) -> list:
    """
    Normalize multiple rows into IndexConstituent objects.

    Args:
        rows: List of raw row dictionaries
        index_key: Index identifier
        index_name: Full index name

    Returns:
        List of valid IndexConstituent objects
    """
    constituents = []
    for row in rows:
        constituent = normalize_row(row, index_key, index_name)
        if constituent:
            constituents.append(constituent)

    # Sort for deterministic output
    constituents.sort(key=lambda x: x.symbol.upper())

    logger.info(f"Normalized {len(constituents)} constituents for {index_name}")
    return constituents
