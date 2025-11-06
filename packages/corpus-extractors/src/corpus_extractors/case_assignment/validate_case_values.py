"""
Validation script for case value assignments.

Verifies that all acceptance criteria are met in the output files.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict


def load_jsonl(file_path: Path):
    """Load JSONL file."""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                data.append(json.loads(line))
    return data


def validate_row_count(quotes_in, quotes_out):
    """Validate that row count is preserved."""
    if len(quotes_in) != len(quotes_out):
        print(f"❌ FAIL: Row count mismatch! Input: {len(quotes_in)}, Output: {len(quotes_out)}")
        return False
    print(f"✓ PASS: Row count preserved ({len(quotes_out)} rows)")
    return True


def validate_schema(quotes_out):
    """Validate that all required fields are present."""
    required_fields = [
        'case_id', 'doc_id', 'quote_text', 'speaker',
        'assigned_case_value', 'value_source', 'preferred_outcome_type',
        'source_outcome_doc_ids', 'source_cash_doc_ids'
    ]
    
    for i, quote in enumerate(quotes_out):
        for field in required_fields:
            if field not in quote:
                print(f"❌ FAIL: Missing field '{field}' in row {i}")
                return False
    
    print(f"✓ PASS: All required fields present in {len(quotes_out)} rows")
    return True


def validate_no_fabricated_values(quotes_out, outcomes, cash_amounts):
    """Validate that no values are fabricated."""
    # Build set of valid values from sources
    valid_outcome_amounts = set()
    for outcome in outcomes:
        amount = outcome.get('amount')
        if amount is not None and amount > 0:
            valid_outcome_amounts.add(amount)
    
    valid_cash_amounts = set()
    for cash in cash_amounts:
        if cash.get('feature_votes', 0) > 0:
            valid_cash_amounts.add(cash.get('value'))
    
    for i, quote in enumerate(quotes_out):
        value = quote['assigned_case_value']
        source = quote['value_source']
        
        if value == 'N/A':
            if source != 'N/A':
                print(f"❌ FAIL: Row {i} has N/A value but non-N/A source: {source}")
                return False
        elif isinstance(value, (int, float)):
            if 'outcome_metadata' in source:
                if value not in valid_outcome_amounts:
                    print(f"❌ FAIL: Row {i} has fabricated outcome value: {value}")
                    return False
            elif source == 'cash_amount.highest_votes':
                if value not in valid_cash_amounts:
                    print(f"❌ FAIL: Row {i} has fabricated cash value: {value}")
                    return False
            else:
                print(f"❌ FAIL: Row {i} has invalid source: {source}")
                return False
    
    print("✓ PASS: No fabricated values detected")
    return True


def validate_case_consistency(quotes_out):
    """Validate that all quotes in a case have the same assigned value."""
    case_values = defaultdict(set)
    
    for quote in quotes_out:
        case_id = quote['case_id']
        value = quote['assigned_case_value']
        source = quote['value_source']
        case_values[case_id].add((value, source))
    
    for case_id, values in case_values.items():
        if len(values) > 1:
            print(f"❌ FAIL: Case {case_id} has inconsistent values: {values}")
            return False
    
    print(f"✓ PASS: All cases have consistent values ({len(case_values)} cases)")
    return True


def validate_priority_logic(quotes_out, outcomes, cash_amounts, preferred_outcome_type):
    """Validate that priority logic is correctly applied (basic checks)."""
    
    # Group output quotes by case
    quotes_by_case = defaultdict(list)
    for quote in quotes_out:
        quotes_by_case[quote['case_id']].append(quote)
    
    # Parse case_id from doc_id helper
    def parse_case_id(doc_id):
        if not doc_id or not isinstance(doc_id, str):
            return None
        parts = doc_id.split('_')
        if len(parts) >= 1 and parts[0]:
            return parts[0]
        return None
    
    # Index outcomes and cash by case
    outcomes_by_case = defaultdict(list)
    for outcome in outcomes:
        case_id = outcome.get('case_id') or parse_case_id(outcome.get('doc_id', ''))
        if case_id:
            outcomes_by_case[case_id].append(outcome)
    
    cash_by_case = defaultdict(list)
    for cash in cash_amounts:
        case_id = cash.get('case_id') or parse_case_id(cash.get('doc_id', ''))
        if case_id:
            cash_by_case[case_id].append(cash)
    
    # Basic validation checks
    errors = 0
    for case_id, case_quotes in quotes_by_case.items():
        if not case_id:
            continue
        
        actual_value = case_quotes[0]['assigned_case_value']
        actual_source = case_quotes[0]['value_source']
        
        # Check Priority 1: preferred outcomes
        case_outcomes = outcomes_by_case.get(case_id, [])
        preferred_outcomes = [
            o for o in case_outcomes
            if o.get('outcome_type') == preferred_outcome_type
            and o.get('amount') is not None
            and o.get('amount') > 0
        ]
        
        if preferred_outcomes:
            # Should use outcome
            if f"outcome_metadata.{preferred_outcome_type}" not in actual_source:
                print(f"❌ FAIL: Case {case_id} has preferred outcome but didn't use it. "
                      f"Source: {actual_source}")
                errors += 1
            continue
        
        # Check Priority 2: cash with votes
        case_cash = cash_by_case.get(case_id, [])
        voted_cash = [c for c in case_cash if c.get('feature_votes', 0) > 0]
        
        if voted_cash:
            # Should use cash
            if actual_source != 'cash_amount.highest_votes':
                print(f"❌ FAIL: Case {case_id} has voted cash but didn't use it. "
                      f"Source: {actual_source}")
                errors += 1
            continue
        
        # Check Priority 3: N/A
        if actual_value != 'N/A' or actual_source != 'N/A':
            print(f"❌ FAIL: Case {case_id} should be N/A but got {actual_value} ({actual_source})")
            errors += 1
    
    if errors > 0:
        print(f"❌ FAIL: {errors} cases have incorrect priority logic")
        return False
    
    print(f"✓ PASS: All cases follow correct priority logic")
    return True


def main():
    """Run all validation checks."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate case value assignments')
    parser.add_argument('--quotes_in', type=Path, required=True, help='Original quotes file')
    parser.add_argument('--quotes_out', type=Path, required=True, help='Output quotes file')
    parser.add_argument('--outcomes', type=Path, required=True, help='Outcomes file')
    parser.add_argument('--cash', type=Path, required=True, help='Cash amounts file')
    parser.add_argument('--preferred_outcome', type=str, default='stipulated_judgment',
                       help='Expected preferred outcome type')
    
    args = parser.parse_args()
    
    print(f"Loading data...")
    quotes_in = load_jsonl(args.quotes_in)
    quotes_out = load_jsonl(args.quotes_out)
    outcomes = load_jsonl(args.outcomes)
    cash_amounts = load_jsonl(args.cash)
    
    print(f"\nValidating {args.quotes_out.name}...\n")
    
    all_pass = True
    
    all_pass &= validate_row_count(quotes_in, quotes_out)
    all_pass &= validate_schema(quotes_out)
    all_pass &= validate_no_fabricated_values(quotes_out, outcomes, cash_amounts)
    all_pass &= validate_case_consistency(quotes_out)
    all_pass &= validate_priority_logic(quotes_out, outcomes, cash_amounts, args.preferred_outcome)
    
    print("\n" + "=" * 60)
    if all_pass:
        print("✓ ALL VALIDATION CHECKS PASSED")
        return 0
    else:
        print("❌ SOME VALIDATION CHECKS FAILED")
        return 1


if __name__ == '__main__':
    sys.exit(main())

