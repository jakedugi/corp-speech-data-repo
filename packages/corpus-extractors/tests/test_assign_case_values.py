"""
Unit tests for assign_case_values module.

Tests cover all acceptance criteria:
1. Stipulated judgment wins over cash amounts
2. Tie-breaking logic for equal votes
3. Zero votes → N/A
4. Non-preferred outcome types ignored
5. Row count preservation
"""

import pytest
from pathlib import Path
import tempfile
import json
from typing import List, Dict, Any

from corpus_extractors.assign_case_values import (
    parse_case_id_from_doc_id,
    normalize_case_ids,
    index_by_case,
    select_preferred_outcomes,
    select_best_outcome,
    select_voted_cash_amounts,
    select_best_cash_amount,
    compute_case_value,
    assign_case_values,
    load_jsonl,
    write_jsonl
)


class TestCaseIdParsing:
    """Test case ID extraction from doc ID."""
    
    def test_parse_standard_doc_id(self):
        """Test parsing standard doc_id format."""
        doc_id = "1:13-cv-00002_dcd_entry_2930836"
        assert parse_case_id_from_doc_id(doc_id) == "1:13-cv-00002"
    
    def test_parse_short_doc_id(self):
        """Test parsing doc_id with minimal format."""
        doc_id = "1:13-cv-00002"
        assert parse_case_id_from_doc_id(doc_id) == "1:13-cv-00002"
    
    def test_parse_empty_doc_id(self):
        """Test handling empty doc_id."""
        assert parse_case_id_from_doc_id("") is None
    
    def test_parse_none_doc_id(self):
        """Test handling None doc_id."""
        assert parse_case_id_from_doc_id(None) is None
    
    def test_normalize_case_ids_with_missing(self):
        """Test normalizing records that lack case_id."""
        records = [
            {'doc_id': '1:13-cv-00002_dcd_entry_123', 'value': 100},
            {'doc_id': '1:23-cv-00475_dcd_entry_456', 'case_id': '1:23-cv-00475', 'value': 200}
        ]
        normalized = normalize_case_ids(records)
        
        assert normalized[0]['case_id'] == '1:13-cv-00002'
        assert normalized[1]['case_id'] == '1:23-cv-00475'


class TestIndexing:
    """Test data indexing by case."""
    
    def test_index_by_case(self):
        """Test grouping records by case_id."""
        records = [
            {'case_id': 'case1', 'value': 100},
            {'case_id': 'case1', 'value': 200},
            {'case_id': 'case2', 'value': 300}
        ]
        
        index = index_by_case(records)
        
        assert len(index) == 2
        assert len(index['case1']) == 2
        assert len(index['case2']) == 1


class TestOutcomeSelection:
    """Test outcome filtering and selection logic."""
    
    def test_select_preferred_outcomes_filters_by_type(self):
        """Test that only preferred outcome type is selected."""
        outcomes = [
            {'outcome_type': 'stipulated_judgment', 'amount': 100, 'doc_id': 'doc1'},
            {'outcome_type': 'settlement', 'amount': 200, 'doc_id': 'doc2'},
            {'outcome_type': 'stipulated_judgment', 'amount': 300, 'doc_id': 'doc3'}
        ]
        
        filtered = select_preferred_outcomes(outcomes, 'stipulated_judgment')
        
        assert len(filtered) == 2
        assert all(o['outcome_type'] == 'stipulated_judgment' for o in filtered)
    
    def test_select_preferred_outcomes_filters_zero_amounts(self):
        """Test that zero amounts are excluded."""
        outcomes = [
            {'outcome_type': 'stipulated_judgment', 'amount': 0, 'doc_id': 'doc1'},
            {'outcome_type': 'stipulated_judgment', 'amount': 100, 'doc_id': 'doc2'},
            {'outcome_type': 'stipulated_judgment', 'amount': None, 'doc_id': 'doc3'}
        ]
        
        filtered = select_preferred_outcomes(outcomes, 'stipulated_judgment')
        
        assert len(filtered) == 1
        assert filtered[0]['amount'] == 100
    
    def test_select_best_outcome_picks_largest(self):
        """Test that best outcome selects largest amount."""
        outcomes = [
            {'amount': 100, 'doc_id': 'doc1'},
            {'amount': 300, 'doc_id': 'doc2'},
            {'amount': 200, 'doc_id': 'doc3'}
        ]
        
        best = select_best_outcome(outcomes)
        
        assert best['amount'] == 300
    
    def test_select_best_outcome_tie_breaks_by_doc_id(self):
        """Test tie-breaking by doc_id when amounts equal."""
        outcomes = [
            {'amount': 100, 'doc_id': 'doc_c'},
            {'amount': 100, 'doc_id': 'doc_a'},
            {'amount': 100, 'doc_id': 'doc_b'}
        ]
        
        best = select_best_outcome(outcomes)
        
        assert best['doc_id'] == 'doc_a'


class TestCashAmountSelection:
    """Test cash amount filtering and selection logic."""
    
    def test_select_voted_cash_amounts_filters_zero_votes(self):
        """Test that zero votes are excluded."""
        cash_amounts = [
            {'value': 100, 'feature_votes': 0, 'doc_id': 'doc1'},
            {'value': 200, 'feature_votes': 1, 'doc_id': 'doc2'},
            {'value': 300, 'feature_votes': 2, 'doc_id': 'doc3'}
        ]
        
        filtered = select_voted_cash_amounts(cash_amounts)
        
        assert len(filtered) == 2
        assert all(ca['feature_votes'] > 0 for ca in filtered)
    
    def test_select_best_cash_amount_picks_highest_votes(self):
        """Test that highest votes wins."""
        cash_amounts = [
            {'value': 5000, 'feature_votes': 3, 'doc_id': 'doc1'},
            {'value': 7000, 'feature_votes': 3, 'doc_id': 'doc2'},
            {'value': 6000, 'feature_votes': 2, 'doc_id': 'doc3'}
        ]
        
        best = select_best_cash_amount(cash_amounts)
        
        # Tie on votes (3), so should pick larger amount
        assert best['value'] == 7000
    
    def test_select_best_cash_amount_tie_breaks_by_amount(self):
        """Test tie-breaking by amount when votes equal."""
        cash_amounts = [
            {'value': 5000, 'feature_votes': 3, 'doc_id': 'doc1'},
            {'value': 7000, 'feature_votes': 3, 'doc_id': 'doc2'}
        ]
        
        best = select_best_cash_amount(cash_amounts)
        
        assert best['value'] == 7000
    
    def test_select_best_cash_amount_full_tie_break(self):
        """Test full tie-breaking: votes, amount, doc_id."""
        cash_amounts = [
            {'value': 5000, 'feature_votes': 3, 'doc_id': 'doc_c'},
            {'value': 5000, 'feature_votes': 3, 'doc_id': 'doc_a'},
            {'value': 5000, 'feature_votes': 3, 'doc_id': 'doc_b'}
        ]
        
        best = select_best_cash_amount(cash_amounts)
        
        assert best['doc_id'] == 'doc_a'


class TestComputeCaseValue:
    """Test case value computation with business rules."""
    
    def test_acceptance_1_stipulated_judgment_wins(self):
        """
        Acceptance Check 1:
        Case with stipulated_judgment amounts {0, 25000} and cash candidates
        → 25000 wins; source = outcome.
        """
        outcomes = [
            {'outcome_type': 'stipulated_judgment', 'amount': 0, 'doc_id': 'doc1'},
            {'outcome_type': 'stipulated_judgment', 'amount': 25000, 'doc_id': 'doc2'}
        ]
        cash_amounts = [
            {'value': 10000, 'feature_votes': 2, 'doc_id': 'doc3'}
        ]
        
        value, source, outcome_docs, cash_docs = compute_case_value(
            'case1', outcomes, cash_amounts, 'stipulated_judgment'
        )
        
        assert value == 25000
        assert source == 'outcome_metadata.stipulated_judgment'
        assert len(outcome_docs) == 1
        assert len(cash_docs) == 0
    
    def test_acceptance_2_cash_with_tie_breaking(self):
        """
        Acceptance Check 2:
        Case with no stipulated_judgment but cash candidates {(5000, 3), (7000, 3), (6000, 2)}
        → 7000 wins (tie on votes → larger amount); source = cash.
        """
        outcomes = []
        cash_amounts = [
            {'value': 5000, 'feature_votes': 3, 'doc_id': 'doc1'},
            {'value': 7000, 'feature_votes': 3, 'doc_id': 'doc2'},
            {'value': 6000, 'feature_votes': 2, 'doc_id': 'doc3'}
        ]
        
        value, source, outcome_docs, cash_docs = compute_case_value(
            'case1', outcomes, cash_amounts, 'stipulated_judgment'
        )
        
        assert value == 7000
        assert source == 'cash_amount.highest_votes'
        assert len(outcome_docs) == 0
        assert len(cash_docs) == 3
    
    def test_acceptance_3_zero_votes_results_in_na(self):
        """
        Acceptance Check 3:
        Case with only cash candidates where all feature_votes == 0
        → N/A.
        """
        outcomes = []
        cash_amounts = [
            {'value': 5000, 'feature_votes': 0, 'doc_id': 'doc1'},
            {'value': 7000, 'feature_votes': 0, 'doc_id': 'doc2'}
        ]
        
        value, source, outcome_docs, cash_docs = compute_case_value(
            'case1', outcomes, cash_amounts, 'stipulated_judgment'
        )
        
        assert value == 'N/A'
        assert source == 'N/A'
        assert len(outcome_docs) == 0
        assert len(cash_docs) == 0
    
    def test_acceptance_4_settlement_ignored(self):
        """
        Acceptance Check 4:
        Case with only outcomes of type settlement and no cash votes > 0
        → N/A (settlement is ignored).
        """
        outcomes = [
            {'outcome_type': 'settlement', 'amount': 50000, 'doc_id': 'doc1'}
        ]
        cash_amounts = [
            {'value': 1000, 'feature_votes': 0, 'doc_id': 'doc2'}
        ]
        
        value, source, outcome_docs, cash_docs = compute_case_value(
            'case1', outcomes, cash_amounts, 'stipulated_judgment'
        )
        
        assert value == 'N/A'
        assert source == 'N/A'
    
    def test_injunctive_relief_preferred(self):
        """Test that injunctive_relief can be preferred outcome type."""
        outcomes = [
            {'outcome_type': 'injunctive_relief', 'amount': 10000000, 'doc_id': 'doc1'},
            {'outcome_type': 'stipulated_judgment', 'amount': 5000, 'doc_id': 'doc2'}
        ]
        cash_amounts = []
        
        value, source, outcome_docs, cash_docs = compute_case_value(
            'case1', outcomes, cash_amounts, 'injunctive_relief'
        )
        
        assert value == 10000000
        assert source == 'outcome_metadata.injunctive_relief'


class TestAssignCaseValues:
    """Test end-to-end case value assignment."""
    
    def test_acceptance_5_row_count_preservation(self):
        """
        Acceptance Check 5:
        Row count preservation: output has same number of rows as input.
        """
        quotes = [
            {'doc_id': 'case1_doc1', 'text': 'Quote 1', 'speaker': 'Speaker A'},
            {'doc_id': 'case1_doc2', 'text': 'Quote 2', 'speaker': 'Speaker B'},
            {'doc_id': 'case2_doc3', 'text': 'Quote 3', 'speaker': 'Speaker C'}
        ]
        outcomes = [
            {'doc_id': 'case1_doc1', 'outcome_type': 'stipulated_judgment', 'amount': 100}
        ]
        cash_amounts = []
        
        enriched = assign_case_values(quotes, outcomes, cash_amounts)
        
        assert len(enriched) == len(quotes)
    
    def test_all_quotes_get_case_value(self):
        """Test that every quote gets assigned a case value."""
        quotes = [
            {'doc_id': 'case1_doc1', 'text': 'Quote 1', 'speaker': 'Speaker A'},
            {'doc_id': 'case1_doc2', 'text': 'Quote 2', 'speaker': 'Speaker B'}
        ]
        outcomes = [
            {'doc_id': 'case1_doc1', 'outcome_type': 'stipulated_judgment', 'amount': 5000}
        ]
        cash_amounts = []
        
        enriched = assign_case_values(quotes, outcomes, cash_amounts)
        
        for quote in enriched:
            assert 'assigned_case_value' in quote
            assert 'value_source' in quote
            assert quote['assigned_case_value'] == 5000
    
    def test_output_schema(self):
        """Test that output has required schema fields."""
        quotes = [
            {'doc_id': 'case1_doc1', 'text': 'Quote 1', 'speaker': 'Speaker A'}
        ]
        outcomes = []
        cash_amounts = []
        
        enriched = assign_case_values(quotes, outcomes, cash_amounts)
        
        required_fields = [
            'case_id',
            'doc_id',
            'quote_text',
            'speaker',
            'assigned_case_value',
            'value_source',
            'preferred_outcome_type',
            'source_outcome_doc_ids',
            'source_cash_doc_ids'
        ]
        
        for field in required_fields:
            assert field in enriched[0]
    
    def test_multiple_cases_handled_independently(self):
        """Test that different cases get independent values."""
        quotes = [
            {'doc_id': 'case1_doc1', 'text': 'Quote 1', 'speaker': 'A'},
            {'doc_id': 'case2_doc2', 'text': 'Quote 2', 'speaker': 'B'}
        ]
        outcomes = [
            {'doc_id': 'case1_doc1', 'outcome_type': 'stipulated_judgment', 'amount': 1000},
            {'doc_id': 'case2_doc2', 'outcome_type': 'stipulated_judgment', 'amount': 2000}
        ]
        cash_amounts = []
        
        enriched = assign_case_values(quotes, outcomes, cash_amounts)
        
        case1_quotes = [q for q in enriched if q['case_id'] == 'case1']
        case2_quotes = [q for q in enriched if q['case_id'] == 'case2']
        
        assert all(q['assigned_case_value'] == 1000 for q in case1_quotes)
        assert all(q['assigned_case_value'] == 2000 for q in case2_quotes)


class TestFileIO:
    """Test file loading and writing."""
    
    def test_load_jsonl(self):
        """Test loading JSONL file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"key": "value1"}\n')
            f.write('{"key": "value2"}\n')
            temp_path = Path(f.name)
        
        try:
            data = load_jsonl(temp_path)
            assert len(data) == 2
            assert data[0]['key'] == 'value1'
            assert data[1]['key'] == 'value2'
        finally:
            temp_path.unlink()
    
    def test_write_jsonl(self):
        """Test writing JSONL file."""
        data = [
            {'key': 'value1'},
            {'key': 'value2'}
        ]
        
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir) / 'output.jsonl'
            write_jsonl(data, temp_path)
            
            # Read back
            loaded = load_jsonl(temp_path)
            assert len(loaded) == 2
            assert loaded[0]['key'] == 'value1'
    
    def test_load_jsonl_handles_malformed(self):
        """Test that malformed JSON is skipped with warning."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"key": "value1"}\n')
            f.write('invalid json\n')
            f.write('{"key": "value2"}\n')
            temp_path = Path(f.name)
        
        try:
            data = load_jsonl(temp_path)
            assert len(data) == 2
            assert data[0]['key'] == 'value1'
            assert data[1]['key'] == 'value2'
        finally:
            temp_path.unlink()


class TestEdgeCases:
    """Test edge cases and guardrails."""
    
    def test_unparseable_case_id_results_in_na(self):
        """Test that unparseable case_id results in N/A assignment."""
        quotes = [
            {'doc_id': '', 'text': 'Quote 1', 'speaker': 'A'}
        ]
        outcomes = []
        cash_amounts = []
        
        enriched = assign_case_values(quotes, outcomes, cash_amounts)
        
        assert len(enriched) == 1
        assert enriched[0]['assigned_case_value'] == 'N/A'
        assert enriched[0]['case_id'] is None
    
    def test_empty_inputs(self):
        """Test handling of empty input lists."""
        enriched = assign_case_values([], [], [])
        assert len(enriched) == 0
    
    def test_mixed_outcome_types_only_preferred_used(self):
        """Test that mixed outcome types only use preferred for value selection."""
        quotes = [
            {'doc_id': 'case1_doc1', 'text': 'Quote 1', 'speaker': 'A'}
        ]
        outcomes = [
            {'doc_id': 'case1_doc1', 'outcome_type': 'settlement', 'amount': 50000},
            {'doc_id': 'case1_doc2', 'outcome_type': 'stipulated_judgment', 'amount': 1000},
            {'doc_id': 'case1_doc3', 'outcome_type': 'verdict', 'amount': 100000}
        ]
        cash_amounts = []
        
        enriched = assign_case_values(quotes, outcomes, cash_amounts, 'stipulated_judgment')
        
        assert enriched[0]['assigned_case_value'] == 1000
        assert enriched[0]['value_source'] == 'outcome_metadata.stipulated_judgment'
    
    def test_case_id_already_present(self):
        """Test that existing case_id is preserved."""
        quotes = [
            {'doc_id': 'case1_doc1', 'case_id': 'custom_case_1', 'text': 'Quote 1', 'speaker': 'A'}
        ]
        outcomes = [
            {'doc_id': 'custom_case_1_doc1', 'case_id': 'custom_case_1',
             'outcome_type': 'stipulated_judgment', 'amount': 1000}
        ]
        cash_amounts = []
        
        enriched = assign_case_values(quotes, outcomes, cash_amounts)
        
        assert enriched[0]['case_id'] == 'custom_case_1'
        assert enriched[0]['assigned_case_value'] == 1000


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

