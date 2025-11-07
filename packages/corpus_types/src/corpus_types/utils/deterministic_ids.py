"""
Deterministic ID generation utilities for stable, reproducible identifiers.

These functions generate IDs that are:
- Deterministic: Same inputs → same outputs
- Collision-resistant: Low probability of duplicate IDs
- Reproducible: Same data produces same ID across runs
"""

import hashlib
import json
from typing import Any, Dict, Optional


def generate_quote_id(doc_id: str, start: int, end: int, text: str) -> str:
    """
    Generate a deterministic quote ID based on document ID and span.

    Args:
        doc_id: Document identifier
        start: Start character position
        end: End character position
        text: Quote text content

    Returns:
        Stable quote identifier
    """
    # Create canonical representation
    canonical_data = {
        "doc_id": doc_id,
        "start": start,
        "end": end,
        "text": text.strip(),
    }

    # Generate hash from canonical JSON
    data_str = json.dumps(canonical_data, sort_keys=True, separators=(",", ":"))
    hash_obj = hashlib.blake2b(data_str.encode("utf-8"), digest_size=16)
    return f"q_{hash_obj.hexdigest()}"


def generate_case_id(doc_id: str, case_info: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate a deterministic case ID based on document and case information.

    Args:
        doc_id: Document identifier
        case_info: Optional case metadata

    Returns:
        Stable case identifier
    """
    if case_info:
        canonical_data = {"doc_id": doc_id, "case_info": case_info}
        data_str = json.dumps(canonical_data, sort_keys=True, separators=(",", ":"))
        hash_obj = hashlib.blake2b(data_str.encode("utf-8"), digest_size=16)
        return f"case_{hash_obj.hexdigest()}"
    else:
        # Fallback to doc_id-based case ID
        hash_obj = hashlib.blake2b(doc_id.encode("utf-8"), digest_size=16)
        return f"case_{hash_obj.hexdigest()}"


def generate_outcome_id(case_id: str, outcome_type: str, confidence: float) -> str:
    """
    Generate a deterministic outcome ID.

    Args:
        case_id: Case identifier
        outcome_type: Type of outcome (win, loss, settlement, etc.)
        confidence: Confidence score

    Returns:
        Stable outcome identifier
    """
    canonical_data = {
        "case_id": case_id,
        "outcome_type": outcome_type,
        "confidence": round(confidence, 4),  # Round for consistency
    }

    data_str = json.dumps(canonical_data, sort_keys=True, separators=(",", ":"))
    hash_obj = hashlib.blake2b(data_str.encode("utf-8"), digest_size=16)
    return f"outcome_{hash_obj.hexdigest()}"


def generate_doc_id(source_uri: str, retrieved_at: str) -> str:
    """
    Generate a deterministic document ID based on source and timestamp.

    Args:
        source_uri: Source URI
        retrieved_at: ISO timestamp string

    Returns:
        Stable document identifier
    """
    canonical_data = {"source_uri": source_uri, "retrieved_at": retrieved_at}

    data_str = json.dumps(canonical_data, sort_keys=True, separators=(",", ":"))
    hash_obj = hashlib.blake2b(data_str.encode("utf-8"), digest_size=16)
    return f"doc_{hash_obj.hexdigest()}"


def sort_records_by_id(records: list, id_field: str = "id") -> list:
    """
    Sort records deterministically by ID for consistent output ordering.

    Args:
        records: List of dictionaries with ID fields
        id_field: Name of the ID field to sort by

    Returns:
        Sorted list of records
    """

    def get_sort_key(record):
        return record.get(id_field, "")

    return sorted(records, key=get_sort_key)


def validate_id_uniqueness(records: list, id_field: str = "id") -> tuple[bool, list]:
    """
    Validate that all records have unique IDs.

    Args:
        records: List of records to validate
        id_field: Name of the ID field

    Returns:
        (is_valid, duplicate_ids)
    """
    seen_ids = set()
    duplicates = []

    for record in records:
        record_id = record.get(id_field)
        if record_id in seen_ids:
            duplicates.append(record_id)
        else:
            seen_ids.add(record_id)

    return len(duplicates) == 0, duplicates
