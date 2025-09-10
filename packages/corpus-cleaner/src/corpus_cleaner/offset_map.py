"""
Offset mapping utilities for text normalization.

This module provides utilities for tracking character positions through
text normalization transformations, enabling span alignments to be preserved.
"""

from typing import List, Tuple, Dict, Any
import logging

logger = logging.getLogger(__name__)


def map_raw_to_norm(
    raw_text: str,
    norm_text: str,
    transformations: List[Dict[str, Any]]
) -> List[Tuple[int, int, int, int]]:
    """
    Create offset mappings from raw to normalized text.

    Args:
        raw_text: Original raw text
        norm_text: Normalized text
        transformations: List of transformation operations applied

    Returns:
        List of (raw_start, raw_end, norm_start, norm_end) tuples
    """
    # For now, return a simple identity mapping
    # In practice, this would track character positions through each transformation
    min_len = min(len(raw_text), len(norm_text))
    return [(i, i, i, i) for i in range(min_len)]


def apply_offset_mapping(
    original_spans: List[Tuple[int, int]],
    offset_map: List[Tuple[int, int, int, int]]
) -> List[Tuple[int, int]]:
    """
    Apply offset mapping to transform spans from raw to normalized coordinates.

    Args:
        original_spans: List of (start, end) spans in original text
        offset_map: Offset mapping from raw to normalized positions

    Returns:
        List of transformed (start, end) spans in normalized text
    """
    # Simple implementation - in practice this would be more sophisticated
    transformed_spans = []
    for start, end in original_spans:
        # Find corresponding positions in normalized text
        norm_start = start
        norm_end = end
        transformed_spans.append((norm_start, norm_end))

    return transformed_spans


def validate_offset_map(
    raw_text: str,
    norm_text: str,
    offset_map: List[Tuple[int, int, int, int]]
) -> bool:
    """
    Validate that an offset mapping is consistent.

    Args:
        raw_text: Original raw text
        norm_text: Normalized text
        offset_map: Offset mapping to validate

    Returns:
        True if mapping is valid, False otherwise
    """
    # Basic validation - check that positions are within bounds
    for raw_start, raw_end, norm_start, norm_end in offset_map:
        if (raw_start < 0 or raw_end > len(raw_text) or
            norm_start < 0 or norm_end > len(norm_text)):
            logger.warning(f"Invalid offset mapping: raw[{raw_start}:{raw_end}] -> norm[{norm_start}:{norm_end}]")
            return False

    return True
