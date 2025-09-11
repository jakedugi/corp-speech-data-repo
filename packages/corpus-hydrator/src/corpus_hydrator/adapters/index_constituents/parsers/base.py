"""
Base Table Parser Interface

This module defines the protocol/interface for table parsers.
Parsers are responsible for converting raw data into structured row data.
"""

from typing import Protocol, Iterable, Mapping, Any, Dict
from abc import abstractmethod


class TableParser(Protocol):
    """
    Protocol for table parsers.

    Parsers are responsible for converting raw provider data
    (HTML, JSON, etc.) into structured row dictionaries with
    canonical column names.
    """

    @abstractmethod
    def parse(self, raw_data: Mapping[str, Any]) -> Iterable[Dict[str, Any]]:
        """
        Parse raw data into structured row dictionaries.

        Args:
            raw_data: Raw data from provider (HTML string, JSON dict, etc.)

        Returns:
            Iterable of dictionaries, each representing a table row
            with canonical column names.

        Raises:
            ParserError: If parsing fails
        """
        pass

    @property
    @abstractmethod
    def supported_formats(self) -> list[str]:
        """List of supported raw data formats (e.g., ['html', 'json'])."""
        pass


class ParserError(Exception):
    """Base exception for parser-related errors."""

    def __init__(self, parser_name: str, message: str):
        self.parser_name = parser_name
        self.message = message
        super().__init__(f"{parser_name} parser failed: {message}")
