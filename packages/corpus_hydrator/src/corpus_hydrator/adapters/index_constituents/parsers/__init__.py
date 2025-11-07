"""
Index Constituents Parsers

This package contains data parsers for converting raw data into structured format.
"""

from .base import ParserError, TableParser
from .html_table import HtmlTableParser

__all__ = [
    "TableParser",
    "ParserError",
    "HtmlTableParser",
]
