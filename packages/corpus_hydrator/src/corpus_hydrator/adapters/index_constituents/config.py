"""
Index Constituents Configuration

This module contains configurations for extracting constituents from different
market indexes, including URLs, table selectors, and column mappings.
"""

from dataclasses import dataclass
from typing import List


@dataclass
class IndexConfig:
    """Configuration for a specific market index."""

    name: str
    url: str
    table_id: str
    table_class: str
    columns: List[str]
    extract_columns: List[str]

    @property
    def table_selector(self) -> str:
        """CSS selector for finding the constituents table."""
        return f"table#{self.table_id}"


# Index-specific configurations
INDEX_CONFIGS = {
    "sp500": IndexConfig(
        name="S&P 500",
        url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        table_id="constituents",
        table_class="wikitable sortable",
        columns=[
            "Symbol",
            "Security",
            "GICS Sector",
            "GICS Sub-Industry",
            "Headquarters Location",
            "Date first added",
            "CIK",
            "Founded",
        ],
        extract_columns=["Symbol", "Security", "GICS Sector", "Date first added"],
    ),
    "dow": IndexConfig(
        name="Dow Jones Industrial Average",
        url="https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
        table_id="constituents",
        table_class="wikitable sortable",
        columns=[
            "Company",
            "Exchange",
            "Symbol",
            "Industry",
            "Date added",
            "Notes",
            "Index weighting",
        ],
        extract_columns=["Symbol", "Company", "Industry", "Date added"],
    ),
    "nasdaq100": IndexConfig(
        name="Nasdaq 100",
        url="https://en.wikipedia.org/wiki/NASDAQ-100",
        table_id="constituents",
        table_class="wikitable sortable",
        columns=["Symbol", "Company", "Industry", "Sub-Industry", "Date added"],
        extract_columns=["Symbol", "Company", "Industry", "Date added"],
    ),
}


def get_index_config(index_name: str) -> IndexConfig:
    """Get configuration for a specific index."""
    if index_name not in INDEX_CONFIGS:
        available = list(INDEX_CONFIGS.keys())
        raise ValueError(f"Unknown index: {index_name}. Available: {available}")

    return INDEX_CONFIGS[index_name]


def get_available_indexes() -> List[str]:
    """Get list of available index names."""
    return list(INDEX_CONFIGS.keys())


def validate_index_name(index_name: str) -> bool:
    """Validate if an index name is supported."""
    return index_name in INDEX_CONFIGS


def normalize_index_name(index_name: str) -> str:
    """Normalize an index name to match config keys."""
    # Handle common variations
    name_lower = index_name.lower().strip()

    # Special cases
    if "s&p" in name_lower or "sp500" in name_lower or "s and p" in name_lower:
        return "sp500"
    elif "dow" in name_lower:
        return "dow"
    elif "nasdaq" in name_lower or "ndx" in name_lower:
        return "nasdaq100"
    else:
        return name_lower
