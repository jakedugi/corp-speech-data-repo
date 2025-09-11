"""
HTML Table Parser

This module provides HTML table parsing functionality for index constituents.
It implements the TableParser protocol for converting HTML content to structured data.
"""

import logging
import re
from typing import Iterable, Mapping, Any, Dict, List

from bs4 import BeautifulSoup

from .base import TableParser, ParserError
from ..config import IndexConfig, get_index_config

logger = logging.getLogger(__name__)


class HtmlTableParser(TableParser):
    """
    HTML table parser for index constituents.

    Parses HTML content containing tables and extracts constituent data
    with intelligent table detection and column mapping.
    """

    def __init__(self, table_selector: str = None):
        """
        Initialize HTML table parser.

        Args:
            table_selector: CSS selector for finding tables (optional)
        """
        self.table_selector = table_selector

    @property
    def supported_formats(self) -> List[str]:
        """Supported raw data formats."""
        return ['html']

    def _find_constituents_table(self, soup: BeautifulSoup, config: IndexConfig) -> Any:
        """Find the constituents table using multiple strategies."""
        # Strategy 1: Find by ID (most specific)
        table = soup.find('table', id=config.table_id)
        if table:
            logger.debug(f"Found table by ID: {config.table_id}")
            return table

        # Strategy 2: Find by class
        tables = soup.find_all('table', class_=lambda x: x and config.table_class in x)
        if tables:
            table = tables[0]  # Use first matching table
            logger.debug(f"Found table by class: {config.table_class}")
            return table

        # Strategy 3: Find any sortable table
        tables = soup.find_all('table', class_=re.compile(r'.*sortable.*'))
        if tables:
            table = tables[0]  # Use first sortable table
            logger.debug("Found table by sortable class")
            return table

        logger.warning("No suitable constituents table found")
        return None

    def _extract_table_headers(self, table: Any) -> List[str]:
        """Extract column headers from the table."""
        headers = []
        header_row = table.find('tr')
        if header_row:
            for cell in header_row.find_all(['th', 'td']):
                header_text = cell.get_text(strip=True)
                if header_text:
                    # Clean header text
                    header_text = re.sub(r'\s+', ' ', header_text)  # Normalize whitespace
                    headers.append(header_text)

        logger.debug(f"Extracted headers: {headers}")
        return headers

    def _extract_table_data(self, table: Any, config: IndexConfig) -> List[Dict[str, Any]]:
        """Extract data rows from the table."""
        rows_data = []
        data_rows = table.find_all('tr')[1:]  # Skip header row

        logger.debug(f"Processing {len(data_rows)} data rows")

        for row_idx, row in enumerate(data_rows):
            cells = row.find_all(['td', 'th'])
            logger.debug(f"Row {row_idx}: {len(cells)} cells")

            if len(cells) >= 2:  # At least symbol/company and one more field
                row_data = {}

                # Extract data based on configured columns
                for i, cell in enumerate(cells):
                    if i < len(config.columns):
                        # Clean cell text
                        text = cell.get_text(strip=True)
                        # Remove citation references like [1], [2], etc.
                        text = re.sub(r'\[\d+\]', '', text)
                        # Normalize whitespace
                        text = ' '.join(text.split())
                        row_data[config.columns[i]] = text

                # Validate we have a symbol or company name
                symbol_field = row_data.get('Symbol') or row_data.get('Company', '')
                if symbol_field and len(symbol_field.strip()) > 0:
                    rows_data.append(row_data)
                    logger.debug(f"Added row with symbol: {symbol_field}")

        logger.debug(f"Extracted {len(rows_data)} valid data rows")
        return rows_data

    def parse(self, raw_data: Mapping[str, Any]) -> Iterable[Dict[str, Any]]:
        """
        Parse HTML content into structured row dictionaries.

        Args:
            raw_data: Raw data from provider containing HTML content

        Returns:
            Iterable of dictionaries, each representing a table row

        Raises:
            ParserError: If parsing fails
        """
        try:
            html_content = raw_data.get('content', '')
            index_key = raw_data.get('index_key', '')

            if not html_content:
                raise ParserError("html_table", "No HTML content provided")

            if not index_key:
                raise ParserError("html_table", "No index_key provided")

            logger.info(f"Parsing HTML content for {index_key}")

            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')

            # Get index configuration
            config = get_index_config(index_key)

            # Find constituents table
            table = self._find_constituents_table(soup, config)
            if not table:
                raise ParserError("html_table", f"No constituents table found for {index_key}")

            # Extract table data
            rows_data = self._extract_table_data(table, config)

            if not rows_data:
                logger.warning(f"No data rows extracted for {index_key}")
                return []

            logger.info(f"Successfully parsed {len(rows_data)} rows for {index_key}")
            return rows_data

        except Exception as e:
            logger.error(f"HTML table parser failed: {e}")
            raise ParserError("html_table", str(e)) from e
