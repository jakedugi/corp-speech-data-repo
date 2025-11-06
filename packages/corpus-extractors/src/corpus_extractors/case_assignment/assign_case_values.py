"""
Deterministic case-level value assignment for quotes.

This module implements authoritative business rules for assigning ONE monetary value
per case and propagating it to every document and quote in that case.

Business Rules:
    1. Inclusion: Include ALL cases, docs, and quotes. Nothing is filtered out.
    2. One value per case: Each case_id gets exactly one assigned_case_value.
    3. Allowed sources ONLY:
       - Priority 1: Non-zero stipulated_judgment (or other preferred outcome type)
       - Priority 2: Cash amount with highest feature_votes (> 0)
       - Fallback: "N/A"
    4. Propagation: The case-level value is attached to every quote in that case.
    5. Never fabricate values: Only assign numeric values from authorized sources.

Example:
    $ python -m corpus_extractors.assign_case_values \\
        --cash data/cash_amounts_extracted_final.jsonl \\
        --outcomes data/outcomes_extracted_final.jsonl \\
        --quotes data/quotes_extracted_final.jsonl \\
        --preferred_outcome stipulated_judgment \\
        --out data/quotes_with_case_values.jsonl \\
        --also_injunctive_relief true
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)


def parse_case_id_from_doc_id(doc_id: str) -> Optional[str]:
    """
    Parse case_id from doc_id using established parsing rule.
    
    The case_id is the prefix before the first underscore.
    
    Args:
        doc_id: Document ID like "1:13-cv-00002_dcd_entry_2930836"
    
    Returns:
        Case ID like "1:13-cv-00002" or None if unparseable
    
    Examples:
        >>> parse_case_id_from_doc_id("1:13-cv-00002_dcd_entry_2930836")
        "1:13-cv-00002"
        >>> parse_case_id_from_doc_id("")
        None
    """
    if not doc_id or not isinstance(doc_id, str):
        return None
    
    parts = doc_id.split('_')
    if len(parts) >= 1 and parts[0]:
        return parts[0]
    
    return None


def load_jsonl(file_path: Path) -> List[Dict[str, Any]]:
    """
    Load JSONL file into list of dictionaries.
    
    Args:
        file_path: Path to JSONL file
    
    Returns:
        List of parsed JSON objects
    """
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if line:
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping malformed JSON at {file_path}:{line_num}: {e}")
    return data


def normalize_case_ids(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Ensure all records have a case_id field, deriving from doc_id when needed.
    
    Args:
        records: List of records that may have case_id and/or doc_id
    
    Returns:
        Records with case_id populated (may be None if unparseable)
    """
    for record in records:
        if 'case_id' not in record or not record['case_id']:
            doc_id = record.get('doc_id', '')
            case_id = parse_case_id_from_doc_id(doc_id)
            record['case_id'] = case_id
            if case_id is None:
                logger.warning(f"Could not parse case_id from doc_id: {doc_id}")
    return records


def index_by_case(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group records by case_id.
    
    Args:
        records: List of records with case_id field
    
    Returns:
        Dictionary mapping case_id to list of records
    """
    index = {}
    for record in records:
        case_id = record.get('case_id')
        if case_id:
            if case_id not in index:
                index[case_id] = []
            index[case_id].append(record)
    return index


def select_preferred_outcomes(
    outcomes: List[Dict[str, Any]],
    preferred_outcome_type: str
) -> List[Dict[str, Any]]:
    """
    Filter outcomes to only those of the preferred type with non-zero amounts.
    
    Args:
        outcomes: List of outcome records for a case
        preferred_outcome_type: Outcome type to prioritize (e.g., "stipulated_judgment")
    
    Returns:
        Filtered list of outcomes matching criteria
    """
    filtered = []
    for outcome in outcomes:
        if outcome.get('outcome_type') == preferred_outcome_type:
            amount = outcome.get('amount')
            if amount is not None and amount > 0:
                filtered.append(outcome)
    return filtered


def select_best_outcome(outcomes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Select the best outcome from a list of valid preferred outcomes.
    
    Tie-breaking: largest amount, then earliest doc_id lexicographically.
    
    Args:
        outcomes: List of valid outcome records
    
    Returns:
        Best outcome or None
    """
    if not outcomes:
        return None
    
    # Sort by: amount desc, doc_id asc
    sorted_outcomes = sorted(
        outcomes,
        key=lambda o: (-o.get('amount', 0), o.get('doc_id', ''))
    )
    return sorted_outcomes[0]


def select_voted_cash_amounts(
    cash_amounts: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Filter cash amounts to only those with feature_votes > 0.
    
    Args:
        cash_amounts: List of cash amount records for a case
    
    Returns:
        Filtered list with feature_votes > 0
    """
    return [ca for ca in cash_amounts if ca.get('feature_votes', 0) > 0]


def select_best_cash_amount(cash_amounts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Select the best cash amount from voted candidates.
    
    Tie-breaking: highest feature_votes, then largest amount, then earliest doc_id.
    
    Args:
        cash_amounts: List of cash amount records with votes > 0
    
    Returns:
        Best cash amount or None
    """
    if not cash_amounts:
        return None
    
    # Sort by: feature_votes desc, amount desc, doc_id asc
    sorted_amounts = sorted(
        cash_amounts,
        key=lambda ca: (
            -ca.get('feature_votes', 0),
            -ca.get('value', 0),
            ca.get('doc_id', '')
        )
    )
    return sorted_amounts[0]


def compute_case_value(
    case_id: str,
    case_outcomes: List[Dict[str, Any]],
    case_cash_amounts: List[Dict[str, Any]],
    preferred_outcome_type: str
) -> Tuple[Any, str, List[str], List[str]]:
    """
    Compute the assigned case value using deterministic business rules.
    
    Rules:
        1. If any preferred outcome with non-zero amount exists, use largest
        2. Else if any cash amount with feature_votes > 0 exists, use highest voted
        3. Else "N/A"
    
    Args:
        case_id: Case identifier
        case_outcomes: All outcomes for this case
        case_cash_amounts: All cash amounts for this case
        preferred_outcome_type: Preferred outcome type (e.g., "stipulated_judgment")
    
    Returns:
        Tuple of (assigned_case_value, value_source, source_outcome_doc_ids, source_cash_doc_ids)
    """
    # Priority 1: Preferred outcome with non-zero amount
    preferred_outcomes = select_preferred_outcomes(case_outcomes, preferred_outcome_type)
    best_outcome = select_best_outcome(preferred_outcomes)
    
    if best_outcome:
        outcome_doc_ids = [o.get('doc_id', '') for o in preferred_outcomes if o.get('doc_id')]
        return (
            best_outcome['amount'],
            f"outcome_metadata.{preferred_outcome_type}",
            outcome_doc_ids,
            []
        )
    
    # Priority 2: Cash amount with feature_votes > 0
    voted_cash_amounts = select_voted_cash_amounts(case_cash_amounts)
    best_cash = select_best_cash_amount(voted_cash_amounts)
    
    if best_cash:
        cash_doc_ids = [ca.get('doc_id', '') for ca in voted_cash_amounts if ca.get('doc_id')]
        return (
            best_cash['value'],
            "cash_amount.highest_votes",
            [],
            cash_doc_ids
        )
    
    # Fallback: N/A
    return ("N/A", "N/A", [], [])


def assign_case_values(
    quotes: List[Dict[str, Any]],
    outcomes: List[Dict[str, Any]],
    cash_amounts: List[Dict[str, Any]],
    preferred_outcome_type: str = "stipulated_judgment"
) -> List[Dict[str, Any]]:
    """
    Assign case-level values to all quotes.
    
    Args:
        quotes: List of quote records
        outcomes: List of outcome records
        cash_amounts: List of cash amount records
        preferred_outcome_type: Outcome type to prioritize
    
    Returns:
        List of quotes with assigned case values
    """
    # Normalize all inputs to ensure case_id is present
    quotes = normalize_case_ids(quotes)
    outcomes = normalize_case_ids(outcomes)
    cash_amounts = normalize_case_ids(cash_amounts)
    
    # Index by case
    outcomes_by_case = index_by_case(outcomes)
    cash_by_case = index_by_case(cash_amounts)
    
    # Get all unique case IDs
    all_case_ids = set(
        [q.get('case_id') for q in quotes if q.get('case_id')]
    )
    
    # Compute case values
    case_value_map = {}
    for case_id in all_case_ids:
        case_outcomes = outcomes_by_case.get(case_id, [])
        case_cash = cash_by_case.get(case_id, [])
        
        value, source, outcome_doc_ids, cash_doc_ids = compute_case_value(
            case_id,
            case_outcomes,
            case_cash,
            preferred_outcome_type
        )
        
        case_value_map[case_id] = {
            'assigned_case_value': value,
            'value_source': source,
            'source_outcome_doc_ids': outcome_doc_ids,
            'source_cash_doc_ids': cash_doc_ids
        }
    
    # Propagate case values to all quotes
    enriched_quotes = []
    for quote in quotes:
        case_id = quote.get('case_id')
        
        # Get case value or default to N/A
        case_value_info = case_value_map.get(case_id, {
            'assigned_case_value': 'N/A',
            'value_source': 'N/A',
            'source_outcome_doc_ids': [],
            'source_cash_doc_ids': []
        })
        
        enriched_quote = {
            'case_id': case_id,
            'doc_id': quote.get('doc_id', ''),
            'quote_text': quote.get('text', ''),
            'speaker': quote.get('speaker', ''),
            'assigned_case_value': case_value_info['assigned_case_value'],
            'value_source': case_value_info['value_source'],
            'preferred_outcome_type': preferred_outcome_type,
            'source_outcome_doc_ids': case_value_info['source_outcome_doc_ids'],
            'source_cash_doc_ids': case_value_info['source_cash_doc_ids']
        }
        
        enriched_quotes.append(enriched_quote)
    
    return enriched_quotes


def write_jsonl(data: List[Dict[str, Any]], file_path: Path) -> None:
    """
    Write data to JSONL file.
    
    Args:
        data: List of dictionaries to write
        file_path: Output file path
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        for record in data:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')


def main():
    """CLI entry point for case value assignment."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Assign case-level monetary values to quotes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--cash',
        type=Path,
        required=True,
        help='Path to cash_amounts_extracted_final.jsonl'
    )
    parser.add_argument(
        '--outcomes',
        type=Path,
        required=True,
        help='Path to outcomes_extracted_final.jsonl'
    )
    parser.add_argument(
        '--quotes',
        type=Path,
        required=True,
        help='Path to quotes_extracted_final.jsonl'
    )
    parser.add_argument(
        '--preferred_outcome',
        type=str,
        default='stipulated_judgment',
        help='Preferred outcome type (default: stipulated_judgment)'
    )
    parser.add_argument(
        '--out',
        type=Path,
        required=True,
        help='Output path for quotes_with_case_values.jsonl'
    )
    parser.add_argument(
        '--also_injunctive_relief',
        type=str,
        choices=['true', 'false'],
        default='false',
        help='Also generate injunctive_relief variant (default: false)'
    )
    parser.add_argument(
        '--log_level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Load input files
    logger.info(f"Loading quotes from {args.quotes}")
    quotes = load_jsonl(args.quotes)
    logger.info(f"Loaded {len(quotes)} quotes")
    
    logger.info(f"Loading outcomes from {args.outcomes}")
    outcomes = load_jsonl(args.outcomes)
    logger.info(f"Loaded {len(outcomes)} outcomes")
    
    logger.info(f"Loading cash amounts from {args.cash}")
    cash_amounts = load_jsonl(args.cash)
    logger.info(f"Loaded {len(cash_amounts)} cash amounts")
    
    # Primary run with preferred outcome type
    logger.info(f"Assigning case values with preferred_outcome={args.preferred_outcome}")
    enriched_quotes = assign_case_values(
        quotes,
        outcomes,
        cash_amounts,
        preferred_outcome_type=args.preferred_outcome
    )
    
    # Write primary output
    logger.info(f"Writing {len(enriched_quotes)} enriched quotes to {args.out}")
    write_jsonl(enriched_quotes, args.out)
    
    # Verify row count preservation
    if len(enriched_quotes) != len(quotes):
        logger.error(
            f"Row count mismatch! Input: {len(quotes)}, Output: {len(enriched_quotes)}"
        )
        raise RuntimeError("Row count preservation failed")
    
    logger.info(f"✓ Row count preserved: {len(enriched_quotes)} quotes")
    
    # Optional: Also generate injunctive_relief variant
    if args.also_injunctive_relief == 'true':
        logger.info("Generating injunctive_relief variant")
        injunctive_quotes = assign_case_values(
            quotes,
            outcomes,
            cash_amounts,
            preferred_outcome_type='injunctive_relief'
        )
        
        # Write injunctive_relief output
        injunctive_out = args.out.parent / f"{args.out.stem}.injunctive_relief{args.out.suffix}"
        logger.info(f"Writing {len(injunctive_quotes)} injunctive_relief quotes to {injunctive_out}")
        write_jsonl(injunctive_quotes, injunctive_out)
        
        logger.info(f"✓ Injunctive relief variant written: {len(injunctive_quotes)} quotes")
    
    logger.info("✓ Case value assignment complete")


if __name__ == '__main__':
    main()

